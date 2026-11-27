use rescrow_application::{
    Code,
    api::{LaneCapacity, Limits},
    authority::TenantId,
};
use rescrow_server::admission::{Control, Lane};
use std::time::Duration;
#[tokio::test]
async fn bounded_queues_tenant_partitions_and_reserved_decisions() {
    let limits = Limits {
        admission: LaneCapacity {
            concurrency: 1,
            queue: 1,
        },
        queue_timeout_ms: 50,
        max_tenants: 2,
        ..Limits::default()
    };
    let control = Control::new(limits).unwrap();
    let tenant = TenantId::new("a").unwrap();
    let active = control.acquire(&tenant, Lane::Admission).await.unwrap();
    let decision = control.acquire(&tenant, Lane::Decision).await.unwrap();
    let read = control.acquire(&tenant, Lane::Recovery).await.unwrap();
    let waiting = {
        let c = control.clone();
        let t = tenant.clone();
        tokio::spawn(async move { c.acquire(&t, Lane::Admission).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        control
            .acquire(&tenant, Lane::Admission)
            .await
            .err()
            .unwrap()
            .code,
        Code::Throttled
    );
    assert_eq!(waiting.await.unwrap().err().unwrap().code, Code::Throttled);
    drop(active);
    control.acquire(&tenant, Lane::Admission).await.unwrap();
    drop(decision);
    drop(read);
    control
        .acquire(&TenantId::new("b").unwrap(), Lane::Recovery)
        .await
        .unwrap();
    assert_eq!(
        control
            .acquire(&TenantId::new("c").unwrap(), Lane::Recovery)
            .await
            .err()
            .unwrap()
            .code,
        Code::Throttled
    );
    control.drain().await;
    assert_eq!(
        control
            .acquire(&tenant, Lane::Decision)
            .await
            .err()
            .unwrap()
            .code,
        Code::Io
    );
}
#[tokio::test]
async fn disabled_admission_preserves_decision_and_recovery_capacity() {
    let mut limits = Limits::default();
    limits.admission.concurrency = 0;
    let control = Control::new(limits).unwrap();
    let tenant = TenantId::new("tenant").unwrap();
    assert_eq!(
        control
            .acquire(&tenant, Lane::Admission)
            .await
            .err()
            .unwrap()
            .code,
        Code::Throttled
    );
    control.acquire(&tenant, Lane::Decision).await.unwrap();
    control.acquire(&tenant, Lane::Recovery).await.unwrap();
}
