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
use vhost_user_blk::backend::StorageBackend;
use vhost_user_blk::backend_raw::StorageBackendRaw;
use vhost_user_blk::backend_raw_async::StorageBackendRawAsync;
use vhost_user_blk::block::{VhostUserBlk, Vring};
use vhost_user_blk::eventfd::EventFd;
use vhost_user_blk::message::*;
use vhostuser_rs::SlaveListener;

const EVENT_SLAVE_IDX: u64 = 0;
const EVENT_VUBMSG_IDX: u64 = 1;
const EVENT_URING_IDX: u64 = 2;

fn read_eventfd(efd: RawFd) {
    let mut buf: u64 = 0;
    unsafe {
        libc::read(
            efd,
            &mut buf as *mut u64 as *mut libc::c_void,
            mem::size_of::<u64>(),
        )
    };
}

enum VringWorkerMsg {
    Stop,
}

struct VringWorkerEndpoint {
    sender: Sender<VringWorkerMsg>,
    eventfd: EventFd,
    join_handle: thread::JoinHandle<()>,
}

struct VringWorker {
    vring: Arc<Mutex<Vring>>,
    image_path: String,
    async_backend: bool,
    poll_ns: u128,
    readonly: bool,
    eventfd: EventFd,
    receiver: Receiver<VringWorkerMsg>,
}

impl VringWorker {
    fn new(
        vring: Arc<Mutex<Vring>>,
        image_path: String,
        async_backend: bool,
        poll_ns: u128,
        readonly: bool,
        eventfd: EventFd,
        receiver: Receiver<VringWorkerMsg>,
    ) -> Self {
        VringWorker {
            vring,
            image_path,
            async_backend,
            poll_ns,
            readonly,
            eventfd,
            receiver,
        }
    }

    fn poll_queues<S: StorageBackend>(&mut self, backend: &mut S) {
        let mut vring = self.vring.lock().unwrap();

        vring.disable_notifications();
        let mut start_time = Instant::now();
        loop {
            if vring.process_completions(backend).unwrap() || vring.process_queue(backend).unwrap()
            {
                start_time = Instant::now();
            }

            if self.poll_ns == 0
                || Instant::now().duration_since(start_time).as_nanos() > self.poll_ns
            {
                vring.enable_notifications();
                vring.process_queue(backend).unwrap();
                break;
            }
        }
    }

    fn main_loop<S: StorageBackend>(
        &mut self,
        mut backend: S,
        epoll_raw_fd: RawFd,
        kick_fd: RawFd,
        uring_efd: EventFd,
    ) {
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
                        read_eventfd(kick_fd);
                        self.poll_queues(&mut backend);
                    }
                    EVENT_URING_IDX => {
                        read_eventfd(uring_efd.as_raw_fd());
                        self.poll_queues(&mut backend);
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

    fn run(&mut self) {
        let uring_efd = EventFd::new().expect("Can't create uring efd");
        let epoll_raw_fd = epoll::create(true).expect("Can't create epoll ofd");
        let kick_fd = self.vring.lock().unwrap().get_kick_fd();

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

        epoll::ctl(
            epoll_raw_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            uring_efd.as_raw_fd(),
            epoll::Event::new(epoll::Events::EPOLLIN, EVENT_URING_IDX),
        )
        .unwrap();

        if self.async_backend {
            let storage_backend = match StorageBackendRawAsync::new(
                &self.image_path,
                uring_efd.as_raw_fd(),
                self.readonly,
                libc::O_DIRECT,
            ) {
                Ok(s) => s,
                Err(e) => {
                    error!("Can't open disk image {}: {}", self.image_path, e);
                    exit(-1);
                }
            };
            self.main_loop(storage_backend, epoll_raw_fd, kick_fd, uring_efd);
        } else {
            let storage_backend =
                match StorageBackendRaw::new(&self.image_path, self.readonly, libc::O_DIRECT) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Can't open disk image {}: {}", self.image_path, e);
                        exit(-1);
                    }
                };
            self.main_loop(storage_backend, epoll_raw_fd, kick_fd, uring_efd);
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
        .arg(
            Arg::with_name("async_backend")
                .long("async_backend")
                .short("a")
                .help("Use an asynchronous backend storage (requires io_uring support)"),
        )
        .arg(
            Arg::with_name("poll_ns")
                .long("poll_ns")
                .short("p")
                .takes_value(true)
                .help("Keep polling the queue for this amount of time (default: 32000)"),
        )
        .arg(
            Arg::with_name("queue_num")
                .long("queue_num")
                .short("q")
                .takes_value(true)
                .help("Number of queues (default: 1)"),
        )
        .arg(
            Arg::with_name("readonly")
                .long("readonly")
                .short("r")
                .help("Open the storage backend in read-only mode"),
        )
        .get_matches();

    let socket_path = cmd_args
        .value_of("socket")
        .expect("Can't parse socket path");
    let disk_image_path = cmd_args
        .value_of("backend")
        .expect("Can't parse backend path");

    let readonly;
    if cmd_args.is_present("readonly") {
        readonly = true;
    } else {
        readonly = false;
    }

    let async_backend;
    if cmd_args.is_present("async_backend") {
        async_backend = true;
    } else {
        async_backend = false;
    }

    let poll_ns;
    if cmd_args.is_present("poll_ns") {
        let poll_ns_str = cmd_args.value_of("poll_ns").expect("Invalid poll_ns value");
        poll_ns = poll_ns_str.parse::<u128>().expect("Invalid poll_ns value");
    } else {
        poll_ns = 32000u128;
    }

    let queue_num: u16;
    if cmd_args.is_present("queue_num") {
        let queue_num_str = cmd_args
            .value_of("queue_num")
            .expect("Invalid queue_num value");
        queue_num = queue_num_str
            .parse::<u16>()
            .expect("Invalid queue_num value");
    } else {
        queue_num = 1;
    }

    let storage_backend = match StorageBackendRaw::new(disk_image_path, true, 0) {
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
        queue_num,
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
                    read_eventfd(main_eventfd.as_raw_fd());

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
                            let mut worker = VringWorker::new(
                                vring,
                                disk_image_path.to_string(),
                                async_backend,
                                poll_ns,
                                readonly,
                                eventfd.try_clone().unwrap(),
                                receiver,
                            );

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
