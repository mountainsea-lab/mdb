use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn unique_test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fdc-server-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    listener.local_addr().expect("listener should expose addr")
}

fn wait_for_server(child: &mut Child, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("child status should be readable") {
            panic!("fdc_server exited before readiness check: {status}");
        }

        match http_get_json(addr, "/health") {
            Ok(json) if json["status"] == "healthy" => return,
            Ok(_) | Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }

    panic!("fdc_server did not become healthy before timeout");
}

fn http_get_json(
    addr: SocketAddr,
    path: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--noproxy")
        .arg("*")
        .arg(format!("http://{addr}{path}"))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "curl failed for {path}: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

fn http_post_json(
    addr: SocketAddr,
    path: &str,
    body: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--noproxy")
        .arg("*")
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-d")
        .arg(body)
        .arg(format!("http://{addr}{path}"))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "curl failed for {path}: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

fn spawn_stderr_reader(child: &mut Child) -> mpsc::Receiver<String> {
    let stderr = child.stderr.take().expect("stderr should be piped");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    receiver
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn production_binary_assembles_tiered_runtime_store() {
    let binary = env!("CARGO_BIN_EXE_fdc_server");
    let addr = free_loopback_addr();
    let root = unique_test_path("binary-runtime-assembly");
    std::fs::create_dir_all(&root).expect("durable root should be created");

    let mut child = Command::new(binary)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("FDC_SERVER_ADDR", addr.to_string())
        .env("FDC_SERVER_ENV", "production")
        .env("FDC_LIVE_ENABLED", "0")
        .env("FDC_LIVE_AUTOSTART", "0")
        .env("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered")
        .env("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime")
        .env("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", root.join("l2.redb"))
        .env("FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH", root.join("l3.duckdb"))
        .env("FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH", root.join("l4-rocksdb"))
        .env("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")
        .env("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("fdc_server binary should start");
    let stderr = spawn_stderr_reader(&mut child);
    let mut child = ChildGuard(child);

    wait_for_server(&mut child.0, addr);

    let version = http_get_json(addr, "/version").expect("version should respond");
    assert_eq!(version["status"], "success");
    assert_eq!(version["data"]["service"], "fdc-server");
    assert_eq!(version["data"]["version"], env!("CARGO_PKG_VERSION"));

    let status = http_get_json(addr, "/market-data/storage/status").expect("status should respond");
    assert_eq!(status["status"], "success");
    assert_eq!(status["data"]["backend"], "tiered");
    assert_eq!(status["data"]["tiered"], true);
    assert_eq!(status["data"]["durable_tiers_configured"], 3);

    let health = http_get_json(addr, "/market-data/storage/health").expect("health should respond");
    assert_eq!(health["status"], "success");
    assert_eq!(health["data"]["backend"], "tiered");
    assert_eq!(health["data"]["tiered"], true);
    assert_eq!(health["data"]["status"], "healthy");
    assert_eq!(health["data"]["tiers"].as_array().unwrap().len(), 4);

    let maintenance = http_post_json(
        addr,
        "/market-data/storage/maintenance/run-once",
        r#"{"confirm":"run_maintenance_once","reason":"binary-contract"}"#,
    )
    .expect("maintenance should respond");
    assert_eq!(maintenance["status"], "success");
    assert_eq!(maintenance["data"]["accepted"], true);
    assert_eq!(maintenance["data"]["status"], "completed");

    let stderr_lines: Vec<String> = stderr.try_iter().collect();
    assert!(
        stderr_lines
            .iter()
            .any(|line| line.contains("fdc server listening on")),
        "server did not print listening line; stderr={stderr_lines:?}"
    );
}
