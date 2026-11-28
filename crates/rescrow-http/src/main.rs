use clap::Parser;
use rescrow_adapters::{authority_memory::MemoryAuthority, file::FileStore};
use rescrow_application::{Application, authority::TransactionalStore, capabilities::Readiness};
use rescrow_http::{
    Api,
    auth::{Issuer, JwtAuthenticator},
    config::Config,
};
use serde::Deserialize;
use std::{io::Read, net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(about = "Rescrow HTTP API v1")]
struct Args {
    /// JSON composition configuration, including trusted public verification keys.
    #[arg(long)]
    config: PathBuf,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Composition {
    listen: SocketAddr,
    backend: Backend,
    api: Config,
    issuers: Vec<Issuer>,
    tls: Option<Tls>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Backend {
    /// Explicit disposable simulation. A fresh authority is initialized on startup.
    Memory,
    /// An existing initialized local authority. Startup never initializes/resets it.
    File { path: PathBuf },
    /// Existing DSQL authority. Schema installation and initialization are explicit.
    #[cfg(feature = "dsql")]
    Dsql { config: rescrow_dsql::Config },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tls {
    certificate: PathBuf,
    private_key: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rescrow_http=info".into()),
        )
        .init();
    let args = Args::parse();
    let mut bytes = Vec::new();
    std::fs::File::open(args.config)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err("configuration exceeds 1 MiB".into());
    }
    let config: Composition = rescrow_application::canonical::decode(&bytes)?;
    if config.tls.is_none() && !config.listen.ip().is_loopback() {
        return Err(
            "plaintext HTTP requires a loopback listener; configure TLS for other addresses".into(),
        );
    }
    match &config.backend {
        Backend::Memory => {
            let store = MemoryAuthority::default();
            store.initialize()?;
            run(store, config).await
        }
        Backend::File { path } => run(FileStore::new(path)?, config).await,
        #[cfg(feature = "dsql")]
        Backend::Dsql { config: dsql } => {
            let dsql = dsql.clone();
            let store = tokio::task::spawn_blocking(move || rescrow_dsql::DsqlStore::connect(dsql))
                .await??;
            let observed = store.clone();
            let monitor = tokio::spawn({
                let observed = observed.clone();
                async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                    loop {
                        interval.tick().await;
                        if let Ok(measurements) = serde_json::to_string(&observed.measurements()) {
                            tracing::info!(target: "rescrow_http::dsql", %measurements, "DSQL cumulative measurements");
                        }
                    }
                }
            });
            let result = run(store, config).await;
            monitor.abort();
            if let Ok(measurements) = serde_json::to_string(&observed.measurements()) {
                tracing::info!(target: "rescrow_http::dsql", %measurements, "DSQL final measurements");
            }
            result
        }
    }
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
async fn run<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    store: S,
    config: Composition,
) -> Result<(), Box<dyn std::error::Error>> {
    let auth = Arc::new(JwtAuthenticator::new(config.issuers)?);
    let api =
        tokio::task::spawn_blocking(move || Api::new(Application::new(store), config.api, auth))
            .await??;
    let listener = std::net::TcpListener::bind(config.listen)?;
    listener.set_nonblocking(true)?;
    tracing::info!(address=%listener.local_addr()?, authority_generation=api.server().authority_generation(), "serving HTTP API");
    if let Some(tls) = config.tls {
        let tls =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(tls.certificate, tls.private_key)
                .await?;
        api.serve_tls(listener, tls, shutdown()).await?;
    } else {
        api.serve(tokio::net::TcpListener::from_std(listener)?, shutdown())
            .await?;
    }
    Ok(())
}
