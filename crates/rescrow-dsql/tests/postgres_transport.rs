//! Actual PostgreSQL protocol response loss, rather than a mocked StoreError.
//! Plaintext is deliberately limited to loopback simulation. Real TLS/DSQL
//! transport and impairment evidence remains a separate qualification gate.
use rescrow_application::{StoreError, authority::*, dto};
use rescrow_dsql::DsqlStore;
use std::{
    io::{self, Read as _, Write as _},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

struct Proxy {
    port: u16,
    armed: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    listener: Option<thread::JoinHandle<()>>,
}
fn packet(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut header = [0; 5];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err(io::Error::other("invalid protocol packet length"));
    }
    let mut packet = vec![0; length + 1];
    packet[..5].copy_from_slice(&header);
    stream.read_exact(&mut packet[5..])?;
    Ok(packet)
}
impl Proxy {
    fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let task = thread::spawn({
            let armed = armed.clone();
            let dropped = dropped.clone();
            let stop = stop.clone();
            let sockets = sockets.clone();
            move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut client, _)) => {
                            let mut server = TcpStream::connect(&upstream).unwrap();
                            client.set_nodelay(true).unwrap();
                            server.set_nodelay(true).unwrap();
                            sockets
                                .lock()
                                .unwrap()
                                .extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);
                            let mut from_server = server.try_clone().unwrap();
                            let mut to_client = client.try_clone().unwrap();
                            let pending = Arc::new(AtomicBool::new(false));
                            let armed = armed.clone();
                            let dropped = dropped.clone();
                            thread::spawn({
                                let pending = pending.clone();
                                move || {
                                    // PostgreSQL startup has a length prefix but no type.
                                    let _ = (|| -> io::Result<()> {
                                        let mut length = [0; 4];
                                        client.read_exact(&mut length)?;
                                        let n = u32::from_be_bytes(length) as usize;
                                        if !(8..=1024 * 1024).contains(&n) {
                                            return Err(io::Error::other("invalid startup"));
                                        }
                                        let mut startup = vec![0; n];
                                        startup[..4].copy_from_slice(&length);
                                        client.read_exact(&mut startup[4..])?;
                                        server.write_all(&startup)?;
                                        loop {
                                            let packet = packet(&mut client)?;
                                            if packet[0] == b'Q'
                                                && &packet[5..] == b"COMMIT\0"
                                                && armed.swap(false, Ordering::SeqCst)
                                            {
                                                pending.store(true, Ordering::SeqCst);
                                            }
                                            server.write_all(&packet)?;
                                        }
                                    })();
                                    let _ = server.shutdown(Shutdown::Both);
                                }
                            });
                            thread::spawn(move || {
                                while let Ok(packet) = packet(&mut from_server) {
                                    if pending.load(Ordering::SeqCst) {
                                        // Wait for ReadyForQuery: the server finished
                                        // COMMIT, but deliver none of its response.
                                        if packet[0] == b'Z' {
                                            dropped.store(true, Ordering::SeqCst);
                                            break;
                                        }
                                    } else if to_client.write_all(&packet).is_err() {
                                        break;
                                    }
                                }
                                let _ = to_client.shutdown(Shutdown::Both);
                                let _ = from_server.shutdown(Shutdown::Both);
                            });
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2))
                        }
                        Err(_) => break,
                    }
                }
            }
        });
        Self {
            port,
            armed,
            dropped,
            stop,
            sockets,
            listener: Some(task),
        }
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.listener.take() {
            task.join().unwrap();
        }
        for socket in self.sockets.lock().unwrap().iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

#[test]
#[ignore = "requires RESCROW_POSTGRES_SIMULATION_URL; loopback plaintext fault test only"]
fn lost_commit_response_is_unknown_and_resolves_to_exact_durable_history() {
    let url = std::env::var("RESCROW_POSTGRES_SIMULATION_URL")
        .expect("explicit PostgreSQL simulation target required");
    let mut url = url::Url::parse(&url).unwrap();
    assert!(
        url.host_str()
            .unwrap()
            .parse::<std::net::IpAddr>()
            .unwrap()
            .is_loopback()
    );
    let upstream = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(5432));
    let proxy = Proxy::start(upstream);
    url.set_host(Some("127.0.0.1")).unwrap();
    url.set_port(Some(proxy.port)).unwrap();
    url.query_pairs_mut().append_pair("sslmode", "disable");
    let store = DsqlStore::postgres_simulation(
        url.as_str(),
        &format!("transport-{}", rescrow_application::new_id().unwrap()),
    )
    .unwrap();
    store.install_schema().unwrap();
    store.initialize().unwrap();
    let tenant = TenantId::local();
    let key = RecordKey::resource("r").unwrap();
    let observed = store.read(&tenant, std::slice::from_ref(&key), 0).unwrap();
    let spec: rescrow_domain::ResourceSpec = dto::ResourceSpec {
        resource_id: "r".into(),
        lower: "0".into(),
        upper: Some("10".into()),
        initial: "10".into(),
        unit: "u".into(),
        scale: 0,
    }
    .try_into()
    .unwrap();
    let commit = Commit::from_changes(
        tenant.clone(),
        TransitionId::new(rescrow_application::uuid_v7(10, 1)).unwrap(),
        &observed,
        &[(10, rescrow_domain::transition::create(spec, None).unwrap())],
    )
    .unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    assert!(matches!(store.commit(&commit), Err(StoreError::Unknown(_))));
    assert!(proxy.dropped.load(Ordering::SeqCst));
    let resolved = store
        .resolve(&tenant, &commit.history().transition_id)
        .unwrap();
    assert!(matches!(resolved,Resolution::Applied {history,..} if history==*commit.history()));
    assert!(
        store
            .read(&tenant, &[key], 0)
            .unwrap()
            .records
            .values()
            .all(Option::is_some)
    );
    assert!(matches!(
        store.commit(&commit).unwrap(),
        CommitOutcome::Applied(_)
    ));
    assert_eq!(store.history(&tenant).unwrap().len(), 1);
}
