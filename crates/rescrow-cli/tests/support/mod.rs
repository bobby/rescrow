use rescrow_adapters::file::FileStore;
use rescrow_application::authority::TransactionalStore;
use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// The existing CLI scenarios use independent client processes against one
/// real file-backed HTTP executable. Credentials come from the documented
/// development helper; the harness adds no HTTP-server or JWT crate dependencies.
pub struct RemoteServer {
    child: Child,
    pub endpoint: String,
    pub generation: String,
}
impl RemoteServer {
    pub fn start(binary: &Path, root: &Path) -> Self {
        let output = Command::new("python3")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/dev-identity.py"))
            .current_dir(root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "development credentials: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let path = root.join(".rescrow-http-dev/config.json");
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        config["listen"] = address.to_string().into();
        config["backend"]["path"] = root.join("store").to_str().unwrap().into();
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let generation = FileStore::new(root.join("store"))
            .unwrap()
            .identity()
            .unwrap();
        drop(listener);
        let child = Command::new(binary)
            .arg("--config")
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut server = Self {
            child,
            endpoint: format!("http://{address}"),
            generation,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                let mut stderr = String::new();
                server
                    .child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                panic!("HTTP executable exited {status}: {stderr}");
            }
            if TcpStream::connect(address).is_ok() {
                return server;
            }
            assert!(
                Instant::now() < deadline,
                "HTTP executable did not bind in time"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for RemoteServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}
