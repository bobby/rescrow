//! Closed-loop authenticated HTTP benchmark. Reuses the production client and
//! its identity-preserving retries. Plans are durable before network dispatch.
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use rescrow_application::{Category, Code, Error, capabilities::DurabilityProfile, dto};
use rescrow_client::{Client, Config};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Barrier;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Workload {
    Independent,
    Hot,
    Skew,
}
#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Simulation,
    Local,
    Regional,
    EnterpriseMr,
}
impl From<Profile> for DurabilityProfile {
    fn from(v: Profile) -> Self {
        match v {
            Profile::Simulation => Self::Simulation,
            Profile::Local => Self::Local,
            Profile::Regional => Self::Regional,
            Profile::EnterpriseMr => Self::EnterpriseMr,
        }
    }
}
#[derive(Clone, Parser, Serialize)]
#[command(
    about = "Reproducible closed-loop HTTP reservation benchmark (creates fresh resources; never resets an authority)"
)]
struct Args {
    #[arg(long, required = true)]
    endpoint: Vec<String>,
    /// Read a short-lived workload token. Contents are never written to results.
    #[arg(long)]
    #[serde(skip_serializing)]
    token_file: PathBuf,
    /// New output directory; existing results are never overwritten.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "simulation")]
    profile: Profile,
    /// Mandatory for multiple endpoints. Must be the actual stored generation.
    #[arg(long)]
    authority_generation: Option<String>,
    #[arg(long, default_value = "independent")]
    workload: Workload,
    #[arg(long, default_value_t = 8)]
    clients: usize,
    #[arg(long, default_value_t = 100)]
    iterations: usize,
    #[arg(long, default_value_t = 20)]
    warmup: usize,
    /// Capacity in each direction, initially centered at zero. Use 10 for near exhaustion.
    #[arg(long, default_value_t = 1_000_000_000)]
    capacity: i64,
    /// Alternate debit and credit requests instead of only debits.
    #[arg(long)]
    opposing: bool,
    #[arg(long, default_value_t = 100)]
    actual_percent: u8,
    /// Null means no automatic expiry.
    #[arg(long)]
    hold_ms: Option<u64>,
    /// Time work waits between preparation and settlement; included in lifecycle latency.
    #[arg(long, default_value_t = 0)]
    settle_delay_ms: u64,
    #[arg(long, default_value_t = 80)]
    hot_percent: u8,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 8)]
    max_attempts: u32,
    #[arg(long, default_value_t = 5000)]
    attempt_timeout_ms: u64,
    #[arg(long, default_value_t = 30000)]
    overall_timeout_ms: u64,
}
#[derive(Clone, Serialize)]
struct Plan {
    worker: usize,
    iteration: usize,
    warmup: bool,
    prepare: dto::Prepare,
    actuals: Vec<dto::Actual>,
}
#[derive(Serialize)]
struct Event {
    worker: usize,
    iteration: usize,
    warmup: bool,
    reservation_id: String,
    prepare_us: u64,
    commit_us: Option<u64>,
    lifecycle_us: u64,
    outcome: &'static str,
    error: Option<Error>,
}
fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
fn histogram<'a>(
    values: impl Iterator<Item = &'a Event>,
    select: impl Fn(&Event) -> Option<u64>,
) -> serde_json::Value {
    let mut hist = Histogram::<u64>::new(3).expect("valid histogram precision");
    for event in values {
        if let Some(value) = select(event) {
            hist.record(value.max(1)).expect("auto-resizing histogram");
        }
    }
    if hist.is_empty() {
        return json!({"samples":0});
    }
    json!({"samples":hist.len(),"p50_us":hist.value_at_quantile(0.50),"p95_us":hist.value_at_quantile(0.95),"p99_us":hist.value_at_quantile(0.99),"max_us":hist.max()})
}
fn write_new(
    path: impl AsRef<std::path::Path>,
    value: &impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
async fn execute(client: &Client, args: &Args, plan: &Plan) -> Event {
    let start = Instant::now();
    let result = client
        .execute(dto::Command::Prepare(plan.prepare.clone()))
        .await;
    let prepare_us = micros(start.elapsed());
    let mut event = Event {
        worker: plan.worker,
        iteration: plan.iteration,
        warmup: plan.warmup,
        reservation_id: plan.prepare.reservation_id.clone(),
        prepare_us,
        commit_us: None,
        lifecycle_us: 0,
        outcome: "failed",
        error: None,
    };
    match result {
        Ok(dto::Output::Reservation(value)) if matches!(value.state, dto::State::Prepared {}) => {
            if args.settle_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(args.settle_delay_ms)).await;
            }
            let commit_start = Instant::now();
            let result = client
                .execute(dto::Command::Commit(dto::Commit {
                    reservation_id: plan.prepare.reservation_id.clone(),
                    actuals: Some(plan.actuals.clone()),
                }))
                .await;
            event.commit_us = Some(micros(commit_start.elapsed()));
            match result {
                Ok(dto::Output::Reservation(value))
                    if matches!(value.state, dto::State::Committed { .. }) =>
                {
                    event.outcome = "committed"
                }
                Ok(_) => event.outcome = "unexpected_response",
                Err(error) => {
                    event.outcome = if error.category == Category::UnknownOutcome {
                        "unknown_commit"
                    } else {
                        "failed_commit"
                    };
                    event.error = Some(error);
                }
            }
        }
        Ok(_) => event.outcome = "unexpected_response",
        Err(error) => {
            event.outcome = if error.category == Category::UnknownOutcome {
                "unknown_prepare"
            } else if matches!(error.code, Code::Capacity | Code::Overflow) {
                "rejected_prepare"
            } else if matches!(error.code, Code::Throttled | Code::Contention) {
                "admission_rejected_prepare"
            } else {
                "failed_prepare"
            };
            event.error = Some(error);
        }
    }
    event.lifecycle_us = micros(start.elapsed());
    event
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !(1..=1024).contains(&args.clients)
        || args.iterations == 0
        || args
            .iterations
            .saturating_add(args.warmup)
            .saturating_mul(args.clients)
            > 1_000_000
        || args.capacity < 10
        || args.actual_percent > 100
        || args.hot_percent > 100
        || args.hold_ms == Some(0)
        || args.hold_ms.is_some_and(|h| h > 30 * 24 * 60 * 60 * 1000)
        || args.settle_delay_ms > 300_000
    {
        return Err("invalid workload bounds".into());
    }
    // Validate configuration before creating anything remotely.
    let bearer = std::fs::read_to_string(&args.token_file)?.trim().to_owned();
    let mut config = Config::new(args.endpoint[0].clone(), bearer);
    config.endpoints = args.endpoint.clone();
    config.required_profile = args.profile.into();
    config.authority_generation = args.authority_generation.clone();
    config.max_attempts = args.max_attempts;
    config.attempt_timeout = Duration::from_millis(args.attempt_timeout_ms);
    config.overall_timeout = Duration::from_millis(args.overall_timeout_ms);
    let client = Arc::new(Client::new(config)?);
    std::fs::create_dir(&args.output)?;
    let run_id = rescrow_application::new_id()?;
    let resources: Vec<_> = (0..args.clients.max(2))
        .map(|i| format!("bench-{run_id}-{i}"))
        .collect();
    let mut plans = Vec::with_capacity(args.clients);
    let mut plan_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.output.join("plans.jsonl"))?;
    for worker in 0..args.clients {
        let mut worker_plans = Vec::new();
        let mut random = args.seed.wrapping_add(worker as u64).wrapping_add(1);
        for iteration in 0..args.warmup + args.iterations {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let resource = match args.workload {
                Workload::Independent => worker,
                Workload::Hot => 0,
                Workload::Skew => {
                    if random % 100 < u64::from(args.hot_percent) {
                        0
                    } else {
                        1 + (random / 100) as usize % (resources.len() - 1)
                    }
                }
            };
            let delta = if args.opposing && (worker + iteration) % 2 == 0 {
                10
            } else {
                -10
            };
            let actual = delta * i64::from(args.actual_percent) / 100;
            let plan = Plan {
                worker,
                iteration,
                warmup: iteration < args.warmup,
                prepare: dto::Prepare {
                    reservation_id: rescrow_application::new_id()?,
                    namespace: None,
                    operations: vec![dto::Operation {
                        operation_id: "op".into(),
                        resource_id: resources[resource].clone(),
                        delta: delta.to_string(),
                    }],
                    hold_ms: args.hold_ms,
                    context: BTreeMap::new(),
                    authorization: None,
                },
                actuals: vec![dto::Actual {
                    operation_id: "op".into(),
                    delta: actual.to_string(),
                }],
            };
            serde_json::to_writer(&mut plan_file, &plan)?;
            plan_file.write_all(b"\n")?;
            worker_plans.push(plan);
        }
        plans.push(worker_plans);
    }
    plan_file.sync_all()?;
    File::open(&args.output)?.sync_all()?;
    write_new(
        args.output.join("configuration.json"),
        &json!({"run_id":run_id,"arguments":args,"resources":resources,"machine":{"os":std::env::consts::OS,"architecture":std::env::consts::ARCH,"available_cpus":std::thread::available_parallelism()?.get()},"started_at_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),"build_profile":if cfg!(debug_assertions) {"debug"} else {"release"},"measurement":"closed_loop; response latency includes client retries; HTTP body bytes exclude headers/TLS/storage; database metrics and cost require separate collection"}),
    )?;
    for resource_id in &resources {
        client
            .execute(dto::Command::CreateResource(dto::ResourceSpec {
                resource_id: resource_id.clone(),
                lower: (-args.capacity).to_string(),
                upper: Some(args.capacity.to_string()),
                initial: "0".into(),
                unit: "benchmark".into(),
                scale: 0,
            }))
            .await?;
    }
    let barrier = Arc::new(Barrier::new(args.clients + 1));
    let warmup_done = Arc::new(Barrier::new(args.clients + 1));
    let mut tasks = Vec::new();
    for worker_plans in plans {
        let client = client.clone();
        let args = args.clone();
        let barrier = barrier.clone();
        let warmup_done = warmup_done.clone();
        tasks.push(tokio::spawn(async move {
            let mut events = Vec::with_capacity(worker_plans.len());
            for plan in &worker_plans[..args.warmup] {
                events.push(execute(&client, &args, plan).await);
            }
            warmup_done.wait().await;
            barrier.wait().await;
            for plan in &worker_plans[args.warmup..] {
                events.push(execute(&client, &args, plan).await);
            }
            events
        }));
    }
    warmup_done.wait().await;
    let before = serde_json::to_value(client.measurements())?;
    let started_at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let start = Instant::now();
    barrier.wait().await;
    let mut events = Vec::new();
    for task in tasks {
        events.extend(task.await?);
    }
    let duration = start.elapsed();
    let finished_at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut measurements = serde_json::to_value(client.measurements())?;
    for (key, value) in measurements.as_object_mut().ok_or("invalid measurements")? {
        *value = json!(
            value
                .as_u64()
                .ok_or("invalid counter")?
                .saturating_sub(before[key].as_u64().ok_or("invalid previous counter")?)
        );
    }
    let measured: Vec<_> = events.iter().filter(|e| !e.warmup).collect();
    let committed = measured.iter().filter(|e| e.outcome == "committed").count();
    let unknown = events
        .iter()
        .filter(|e| e.outcome.starts_with("unknown_"))
        .count();
    let failed_commits = events
        .iter()
        .filter(|e| {
            matches!(
                e.outcome,
                "failed_prepare" | "failed_commit" | "unexpected_response"
            )
        })
        .count();
    let mut outcomes = BTreeMap::new();
    let mut errors = BTreeMap::new();
    for event in &measured {
        *outcomes.entry(event.outcome).or_insert(0usize) += 1;
        if let Some(error) = &event.error {
            *errors.entry(format!("{:?}", error.code)).or_insert(0usize) += 1;
        }
    }
    let logical_calls = measured.len() + measured.iter().filter(|e| e.commit_us.is_some()).count();
    let summary = json!({
        "run_id":run_id,
        "authority_generation":client.authority_generation(),
        "measurement_started_at_ms":started_at_ms,
        "measurement_finished_at_ms":finished_at_ms,
        "duration_seconds":duration.as_secs_f64(),
        "completed_reservations":committed,
        "completed_reservations_per_second":committed as f64/duration.as_secs_f64(),
        "complete":unknown==0 && failed_commits==0,
        "unknown_outcomes_including_warmup":unknown,
        "outcomes":outcomes,
        "errors":errors,
        "prepare":histogram(measured.iter().copied(),|e|Some(e.prepare_us)),
        "successful_prepare":histogram(measured.iter().copied().filter(|e|e.commit_us.is_some()),|e|Some(e.prepare_us)),
        "commit":histogram(measured.iter().copied(),|e|e.commit_us),
        "successful_commit":histogram(measured.iter().copied().filter(|e|e.outcome=="committed"),|e|e.commit_us),
        "lifecycle":histogram(measured.iter().copied(),|e|Some(e.lifecycle_us)),
        "logical_http_calls":logical_calls,
        "http_attempts_per_logical_call":measurements["attempts"].as_u64().unwrap_or(0) as f64/logical_calls as f64,
        "http":measurements,
        "storage_requests_per_decision":null,
        "storage_bytes_per_decision":null,
        "storage_conflict_rate":null,
        "cost_per_million_decisions":null,
        "limitations":[
            "Closed-loop latency; no open-loop arrival SLO qualification.",
            "One authenticated tenant; its outstanding-hold counter can contend across independent resources.",
            "Storage counters, billing, server limits/configuration, and cluster topology must accompany this client report.",
            "Plans retain every identity for reconciliation. Unresolved or failed settlements make complete=false."
        ]
    });
    let mut event_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.output.join("events.jsonl"))?;
    for event in &events {
        serde_json::to_writer(&mut event_file, event)?;
        event_file.write_all(b"\n")?;
    }
    event_file.sync_all()?;
    write_new(args.output.join("summary.json"), &summary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if unknown > 0 || failed_commits > 0 {
        return Err("run has unsettled or unconfirmed outcomes; reconcile original identities in plans.jsonl".into());
    }
    Ok(())
}
