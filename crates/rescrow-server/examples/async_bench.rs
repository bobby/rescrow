//! Development spike, not an SLO. Includes async admission and blocking-pool dispatch.
use rescrow_adapters::authority_memory::MemoryAuthority;
use rescrow_application::{
    Code, authority::TransactionalStore, capabilities::DurabilityProfile, dto, system_clock,
    transactional::Application, uuid_v7,
};
use rescrow_server::Server;
use std::{sync::Arc, time::Instant};
use tokio::sync::Barrier;
const PROFILE: DurabilityProfile = DurabilityProfile::Simulation;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let pairs: usize = std::env::args()
        .nth(1)
        .map(|v| v.parse().expect("pairs per client"))
        .unwrap_or(250);
    assert!(pairs > 0);
    println!(
        "mode,clients,operations,elapsed_ms,operations_per_second,p50_us,p99_us,contention_retries"
    );
    for shared in [false, true] {
        for clients in [1, 8, 32] {
            let store = MemoryAuthority::default();
            store.initialize().unwrap();
            let app = Application::new(store);
            for i in 0..if shared { 1 } else { clients } {
                app.create_resource(dto::ResourceSpec {
                    resource_id: format!("r{i}"),
                    lower: "0".into(),
                    upper: Some("1000".into()),
                    initial: "1000".into(),
                    unit: "u".into(),
                    scale: 0,
                })
                .unwrap();
            }
            let server = Server::start(app, PROFILE, clients).unwrap();
            let barrier = Arc::new(Barrier::new(clients + 1));
            let mut jobs = Vec::new();
            for client in 0..clients {
                let (server, barrier) = (server.clone(), barrier.clone());
                jobs.push(tokio::spawn(async move {
                    let mut latencies = Vec::new();
                    let mut retries = 0;
                    barrier.wait().await;
                    for n in 0..pairs {
                        let id = uuid_v7(system_clock(), ((client as u128) << 48) | n as u128);
                        let resource = format!("r{}", if shared { 0 } else { client });
                        let commands = [
                            dto::Command::Prepare(dto::Prepare {
                                context: Default::default(),
                                authorization: None,
                                transaction_id: id.clone(),
                                namespace: None,
                                operations: vec![dto::Operation {
                                    operation_id: "op".into(),
                                    resource_id: resource,
                                    delta: "-1".into(),
                                }],
                                hold_ms: None,
                            }),
                            dto::Command::Abort { transaction_id: id },
                        ];
                        for command in commands {
                            let begin = Instant::now();
                            loop {
                                match server.execute(command.clone(), PROFILE).await {
                                    Ok(_) => break,
                                    Err(e) if e.code == Code::Contention => {
                                        retries += 1;
                                        assert!(begin.elapsed().as_secs() < 60);
                                    }
                                    Err(e) => panic!("{e}"),
                                }
                            }
                            latencies.push(begin.elapsed().as_micros());
                        }
                    }
                    (latencies, retries)
                }));
            }
            let begin = Instant::now();
            barrier.wait().await;
            let mut samples = Vec::new();
            let mut retries = 0;
            for job in jobs {
                let (latencies, count) = job.await.unwrap();
                samples.extend(latencies);
                retries += count;
            }
            let elapsed = begin.elapsed();
            samples.sort_unstable();
            println!(
                "{},{clients},{},{},{:.1},{},{},{retries}",
                if shared { "shared" } else { "disjoint" },
                samples.len(),
                elapsed.as_millis(),
                samples.len() as f64 / elapsed.as_secs_f64(),
                samples[(samples.len() - 1) / 2],
                samples[(samples.len() * 99).div_ceil(100) - 1]
            );
        }
    }
}
