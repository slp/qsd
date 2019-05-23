// Copyright (C) 2019 Red Hat, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::mem;
use std::os::unix::io::{AsRawFd, RawFd};
use std::process::exit;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use clap::{crate_authors, crate_version, App, Arg};
use log::{debug, error, info};
use vhost_user_blk::backend_raw::StorageBackendRaw;
use vhost_user_blk::block::VhostUserBlk;
use vhost_user_blk::eventfd::EventFd;
use vhost_user_blk::message::*;
use vhost_user_blk::vring::{Vring, VringFd};
use vhostuser_rs::SlaveListener;

const EVENT_SLAVE_IDX: u64 = 0;
const EVENT_VUBMSG_IDX: u64 = 1;

enum VringWorkerMsg {
    Stop,
}

struct VringWorkerEndpoint {
    sender: Sender<VringWorkerMsg>,
    eventfd: EventFd,
    join_handle: thread::JoinHandle<()>,
}

struct VringWorker {
    vring: Arc<Mutex<Vring<StorageBackendRaw>>>,
    eventfd: EventFd,
    receiver: Receiver<VringWorkerMsg>,
}

impl VringWorker {
    fn new(
        vring: Arc<Mutex<Vring<StorageBackendRaw>>>,
        eventfd: EventFd,
        receiver: Receiver<VringWorkerMsg>,
    ) -> Self {
        VringWorker {
            vring,
            eventfd,
            receiver,
        }
    }

    fn get_kick_fd(&self) -> RawFd {
        let vring = self.vring.lock().unwrap();
        vring.get_fd(VringFd::Kick).unwrap()
    }

    fn run(&mut self) {
        let epoll_raw_fd = epoll::create(true).expect("Can't create epoll ofd");
        let kick_fd = self.get_kick_fd();

        epoll::ctl(
            epoll_raw_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            kick_fd,
            epoll::Event::new(epoll::Events::EPOLLIN, EVENT_SLAVE_IDX),
        )
        .unwrap();

        epoll::ctl(
            epoll_raw_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            self.eventfd.as_raw_fd(),
            epoll::Event::new(epoll::Events::EPOLLIN, EVENT_VUBMSG_IDX),
        )
        .unwrap();

        debug!("vring working entering loop");
        loop {
            const EPOLL_EVENTS_LEN: usize = 100;
            let mut events = vec![epoll::Event::new(epoll::Events::empty(), 0); EPOLL_EVENTS_LEN];
            let num_events = match epoll::wait(epoll_raw_fd, -1, &mut events[..]) {
                Ok(events) => events,
                Err(_) => continue,
            };

            for event in events.iter().take(num_events) {
                let idx = event.data as u64;
                match idx {
                    EVENT_SLAVE_IDX => {
                        debug!("worker slave_idx");
                        let mut buf: u64 = 0;
                        unsafe {
                            libc::read(
                                kick_fd,
                                &mut buf as *mut u64 as *mut libc::c_void,
                                mem::size_of::<u64>(),
                            )
                        };

                        let start_time = Instant::now();
                        loop {
                            let mut vring = self.vring.lock().unwrap();
                            let before_queue = Instant::now();
                            let ret = vring.process_queue().unwrap();
                            info!(
                                "process_queue: {}ns, requests={}",
                                Instant::now().duration_since(before_queue).as_nanos(),
                                ret
                            );
                            if ret {
                                match vring.signal_guest() {
                                    Ok(_) => (),
                                    Err(err) => error!("error signaling guest: {:?}", err),
                                }
                            }

                            let now = Instant::now();
                            if now.duration_since(start_time).as_micros() > 30 {
                                break;
                            }
                        }
                    }
                    EVENT_VUBMSG_IDX => {
                        self.eventfd.read().unwrap();
                        match self.receiver.recv().unwrap() {
                            VringWorkerMsg::Stop => {
                                debug!("vring working stopping at main thread request");
                                return ();
                            }
                        }
                    }
                    _ => {
                        panic!("unknown index event!");
                    }
                }
            }
        }
    }
}

fn main() {
    env_logger::init();

    let cmd_args = App::new("qsd")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Serve a vhost-user-blk device for QEMU.")
        .arg(
            Arg::with_name("socket")
                .long("socket")
                .short("s")
                .takes_value(true)
                .required(true)
                .help("Listen in this UNIX socket"),
        )
        .arg(
            Arg::with_name("backend")
                .long("backend")
                .short("b")
                .takes_value(true)
                .required(true)
                .help("Use this raw image or block device as backend"),
        )
        .get_matches();

    let socket_path = cmd_args
        .value_of("socket")
        .expect("Can't parse socket path");
    let disk_image_path = cmd_args
        .value_of("backend")
        .expect("Can't parse backend path");

    let storage_backend = match StorageBackendRaw::new(disk_image_path) {
        Ok(s) => s,
        Err(e) => {
            error!("Can't open disk image {}: {}", disk_image_path, e);
            exit(-1);
        }
    };

    let main_eventfd = EventFd::new().expect("Can't create main eventfd");
    let (main_sender, main_receiver) = channel();

    let vub = Arc::new(Mutex::new(VhostUserBlk::new(
        storage_backend,
        main_eventfd.try_clone().unwrap(),
        main_sender,
        1,
    )));
    let mut slave_listener = SlaveListener::new(&socket_path, vub.clone())
        .expect("Can't create a slave listener instance");

    // Wait for a connection on the UNIX socket
    info!("Waiting for a connection on {}", socket_path);
    let mut slave = slave_listener.accept().unwrap().unwrap();

    let epoll_raw_fd = epoll::create(true).expect("Can't create epoll ofd");

    // Add the slave fd to listen for master requests on the socket
    epoll::ctl(
        epoll_raw_fd,
        epoll::ControlOptions::EPOLL_CTL_ADD,
        slave.as_raw_fd(),
        epoll::Event::new(epoll::Events::EPOLLIN, EVENT_SLAVE_IDX),
    )
    .unwrap();

    // This fd is used by VhostUserBlk to kick us after sending a message
    epoll::ctl(
        epoll_raw_fd,
        epoll::ControlOptions::EPOLL_CTL_ADD,
        main_eventfd.as_raw_fd(),
        epoll::Event::new(epoll::Events::EPOLLIN, EVENT_VUBMSG_IDX),
    )
    .unwrap();

    let mut worker_endpoints: HashMap<usize, VringWorkerEndpoint> = HashMap::new();
    loop {
        const EPOLL_EVENTS_LEN: usize = 100;
        let mut events = vec![epoll::Event::new(epoll::Events::empty(), 0); EPOLL_EVENTS_LEN];
        let num_events = match epoll::wait(epoll_raw_fd, -1, &mut events[..]) {
            Ok(events) => events,
            Err(_) => continue,
        };

        for event in events.iter().take(num_events) {
            let idx = event.data as u64;
            match idx {
                EVENT_SLAVE_IDX => match slave.handle_request() {
                    Ok(_) => (),
                    Err(err) => {
                        error!("slave request failed: {:?}", err);
                        exit(-1);
                    }
                },
                EVENT_VUBMSG_IDX => {
                    let mut buf: u64 = 0;
                    unsafe {
                        libc::read(
                            main_eventfd.as_raw_fd(),
                            &mut buf as *mut u64 as *mut libc::c_void,
                            mem::size_of::<u64>(),
                        )
                    };

                    match main_receiver.recv().unwrap() {
                        VubMessage::EnableVring(msg) => {
                            println!("should enable vring {}", msg.index);
                            let (sender, receiver) = channel();
                            let eventfd = EventFd::new().expect("Can't create main eventfd");
                            let vring = vub
                                .lock()
                                .unwrap()
                                .get_vring(msg.index)
                                .expect("Can't get vring");
                            let mut worker =
                                VringWorker::new(vring, eventfd.try_clone().unwrap(), receiver);

                            let join_handle = thread::spawn(move || worker.run());
                            worker_endpoints.insert(
                                msg.index,
                                VringWorkerEndpoint {
                                    sender,
                                    eventfd,
                                    join_handle,
                                },
                            );
                        }
                        VubMessage::DisableVring(msg) => {
                            println!("should disable vring {}", msg.index);
                            let endpoint = worker_endpoints.remove(&msg.index).unwrap();

                            endpoint.sender.send(VringWorkerMsg::Stop).unwrap();
                            endpoint.eventfd.write(1).unwrap();
                            endpoint.join_handle.join().unwrap();
                        }
                    }
                }
                _ => panic!("unknown event index!"),
            }
        }
    }
}
