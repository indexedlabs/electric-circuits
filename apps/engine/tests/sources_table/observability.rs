//! Source-scoped telemetry contracts through HTTP and UDP, with real PostgreSQL fixtures.
use super::*;

#[derive(Clone, Copy)]
enum Case {
    Metrics,
    Settle,
    Prometheus,
    Statsd,
}

fn run(name: &str, case: Case) -> Result<()> {
    if isolated_contract(name, |_| {})? {
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?.block_on(async {
        let mut f = Fixture::new().await?;
        f.config.dbsp.counts.clear();
        f.config.backfill.append_bytes = 1;
        pg::set_backfill_config(f.config.backfill);
        let result = match case {
            Case::Metrics => metrics(&f).await,
            Case::Settle => settle(&f).await,
            Case::Prometheus => prometheus(&f).await,
            Case::Statsd => statsd(&f).await,
        };
        result.and(f.cleanup().await)
    })
}

#[test]
#[ignore = "PostgreSQL 18 via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn source_metrics_activity_reset_and_shutdown_are_isolated() -> Result<()> {
    run("ownership::observability::source_metrics_activity_reset_and_shutdown_are_isolated", Case::Metrics)
}
#[test]
#[ignore = "PostgreSQL 18 via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn source_settle_checks_are_isolated() -> Result<()> {
    run("ownership::observability::source_settle_checks_are_isolated", Case::Settle)
}
#[test]
#[ignore = "PostgreSQL 18 via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn source_prometheus_series_end_with_the_engine() -> Result<()> {
    run("ownership::observability::source_prometheus_series_end_with_the_engine", Case::Prometheus)
}
#[test]
#[ignore = "PostgreSQL 18 via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn source_statsd_uses_the_source_stack_id() -> Result<()> {
    run("ownership::observability::source_statsd_uses_the_source_stack_id", Case::Statsd)
}

async fn read(app: &Router, source: &str, path: &str) -> Result<serde_json::Value> {
    let (code, body) = json(app, Method::GET, &format!("/sources/{source}/{path}"), Body::empty()).await?;
    ensure!(code == StatusCode::OK, "{path}: {code} {body}");
    Ok(body)
}

async fn metrics(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        let before = read(&app, "beta", "metrics").await?;
        f.snapshot(&app, "/sources/alpha", false).await?;
        let a = read(&app, "alpha", "metrics").await?;
        let after = read(&app, "beta", "metrics").await?;
        let mut failures = Vec::new();
        ensure!(
            a["counters"]["backfill_chunked_appends_total"].as_u64().unwrap_or(0) > 0,
            "fixture must produce chunked backfill activity"
        );
        if before["counters"] != after["counters"] {
            failures.push(format!(
                "alpha activity changed beta counters: before={} after={}",
                before["counters"], after["counters"]
            ));
        }
        f.snapshot(&app, "/sources/beta", false).await?;
        let before_reset = read(&app, "beta", "metrics").await?;
        let (code, _) = json(&app, Method::POST, "/sources/alpha/metrics/reset", Body::empty()).await?;
        ensure!(code == StatusCode::OK, "reset status {code}");
        let after_reset = read(&app, "beta", "metrics").await?;
        if before_reset["counters"] != after_reset["counters"] {
            failures.push(format!(
                "reset alpha changed beta counters: before={} after={}",
                before_reset["counters"], after_reset["counters"]
            ));
        }
        f.restart(&host, 2).await?;
        let after_restart = read(&app, "beta", "metrics").await?;
        if after_restart["gauges"]["shutdown_in_progress"] != 0 {
            failures.push(format!("alpha restart marked beta shutting down: {}", after_restart["gauges"]));
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn settle(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        let before_a = read(&app, "alpha", "replication/lsn").await?;
        let before_b = read(&app, "beta", "replication/lsn").await?;
        f.snapshot(&app, "/sources/alpha", false).await?;
        let after_a = read(&app, "alpha", "replication/lsn").await?;
        let after_b = read(&app, "beta", "replication/lsn").await?;
        ensure!(
            after_a["settle"]["checks"].as_u64() > before_a["settle"]["checks"].as_u64(),
            "alpha snapshot must check its settle record"
        );
        ensure!(
            after_b["settle"]["checks"] == before_b["settle"]["checks"],
            "alpha snapshot changed beta settle checks: before={} after={}",
            before_b["settle"],
            after_b["settle"]
        );
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn scrape(app: &Router, source: &str) -> Result<String> {
    let response = app
        .clone()
        .oneshot(axum::http::Request::get(format!("/sources/{source}/metrics/prometheus")).body(Body::empty())?)
        .await?;
    ensure!(response.status() == StatusCode::OK, "Prometheus status {}", response.status());
    Ok(String::from_utf8(to_bytes(response.into_body(), 1024 * 1024).await?.to_vec())?)
}

async fn prometheus(f: &Fixture) -> Result<()> {
    // The production composition root initializes the process provider before sources start.
    let _provider = electric_circuits_engine::mem::init_otel();
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        f.snapshot(&app, "/sources/alpha", false).await?;
        read(&app, "alpha", "memory").await?;
        read(&app, "beta", "memory").await?;
        let initial = scrape(&app, "alpha").await?;
        ensure!(
            initial.contains("source_id=\"alpha\""),
            "source Prometheus must contain alpha series with source_id: {initial}"
        );
        ensure!(!initial.contains("source_id=\"beta\""), "alpha scrape includes beta");
        let baseline = initial.lines().filter(|line| line.starts_with("engine_")).count();
        let mut old_states = Vec::new();
        for revision in 2..=51 {
            old_states.push(host.engine_for_test("alpha").await.context("alpha missing")?.state_alive_probe_for_test());
            f.restart(&host, revision).await?;
            let current = scrape(&app, "alpha").await?;
            ensure!(
                current.lines().filter(|line| line.starts_with("engine_")).count() == baseline,
                "restart {} accumulated source series",
                revision - 1
            );
            ensure!(old_states.iter().all(|probe| !probe()), "restart {} retained an old Engine", revision - 1);
        }
        f.client.execute(&format!("DELETE FROM {} WHERE source_id = 'alpha'", f.sources_table), &[]).await?;
        set_revision(&f.client, &f.version_table, 52).await?;
        host.refresh().await?;
        let all = electric_circuits_engine::mem::prometheus_text();
        ensure!(!all.contains("source_id=\"alpha\""), "stopped source series remain");
        ensure!(all.contains("source_id=\"beta\""), "stopping alpha removed beta series");
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn statsd(f: &Fixture) -> Result<()> {
    use electric_circuits_engine::{config::StatsdTarget, statsd};
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    statsd::init(&StatsdTarget { host: "127.0.0.1".into(), port: socket.local_addr()?.port() }, &f.config.instance_id);
    let host = f.host().await?;
    let result = async {
        let app = host.router();
        f.snapshot(&app, "/sources/alpha", false).await?;
        f.snapshot(&app, "/sources/beta", false).await?;
        let lines = tokio::time::timeout(Duration::from_secs(5), async {
            let mut responses = Vec::new();
            let mut bytes = [0u8; 4096];
            while responses.len() < 2 {
                let n = socket.recv(&mut bytes).await?;
                for line in std::str::from_utf8(&bytes[..n])?.lines() {
                    if line.starts_with("electric.shape.response_size.bytes:") {
                        responses.push(line.to_string());
                    }
                }
            }
            Ok::<_, anyhow::Error>(responses)
        })
        .await
        .context("waiting for the two completed snapshot response metrics")??;
        ensure!(
            lines[0].contains("stack_id:alpha") && lines[1].contains("stack_id:beta"),
            "source responses need their own stack_id: {lines:?}"
        );
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}
