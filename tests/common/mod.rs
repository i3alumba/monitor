use anyhow::{Context, Result};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

pub struct Fixture {
    dir: tempfile::TempDir,
    child: Child,
    pub url: String,
}
impl std::ops::Deref for Fixture {
    type Target = tempfile::TempDir;
    fn deref(&self) -> &Self::Target {
        &self.dir
    }
}
impl Fixture {
    async fn start(dir: tempfile::TempDir, requested_port: Option<u16>) -> Result<Self> {
        let executable = std::env::var("REDIS_SERVER").unwrap_or_else(|_| "redis-server".into());
        let socket = dir.path().join("redis.sock");
        // Start with TCP disabled. Configure a port through the unique Unix
        // socket so releasing a probe listener cannot select another fixture.
        let child = Command::new(executable)
            .args(["--bind", "127.0.0.1", "--port", "0", "--save", "", "--appendonly", "no", "--maxmemory", "64mb", "--maxmemory-policy", "volatile-lru", "--unixsocket"])
            .arg(&socket).arg("--dir").arg(dir.path())
            .stdout(Stdio::null()).stderr(Stdio::null()).spawn()
            .context("real Redis tests require redis-server; install test tooling or set REDIS_SERVER (tests never skip)")?;
        let mut fixture = Self {
            dir,
            child,
            url: String::new(),
        };
        let client = redis::Client::open(format!("unix://{}", socket.display()))?;
        for _ in 0..100 {
            if let Some(status) = fixture.child.try_wait()? {
                anyhow::bail!("test Redis exited: {status}");
            }
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                for _ in 0..100 {
                    let port = if let Some(port) = requested_port {
                        port
                    } else {
                        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
                        listener.local_addr()?.port()
                    };
                    let result: redis::RedisResult<()> = redis::cmd("CONFIG")
                        .arg("SET")
                        .arg("port")
                        .arg(port)
                        .query_async(&mut conn)
                        .await;
                    if result.is_ok() {
                        fixture.url = format!("redis://127.0.0.1:{port}/0");
                        return Ok(fixture);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                anyhow::bail!("test Redis could not reserve TCP port");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("test Redis did not start")
    }
    pub async fn new() -> Result<Self> {
        Self::start(tempfile::tempdir()?, None).await
    }
    #[allow(dead_code)] // Shared by separate integration-test binaries.
    pub async fn restart(&mut self) -> Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        let port = reqwest::Url::parse(&self.url)?
            .port()
            .context("test Redis port")?;
        let replacement = Self::start(tempfile::tempdir()?, Some(port)).await?;
        *self = replacement;
        Ok(())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
