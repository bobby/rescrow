use crate::{auth::Authenticator, config::Config, cursor::Cursors};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Extension, MatchedPath, Query, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rescrow_application::{
    Application, Category, Code, Error,
    access::{AccessContext, Action},
    api::{self, Envelope, Metadata, Page},
    authority::{RecordKey, TransactionalStore},
    canonical,
    capabilities::{DurabilityProfile, Readiness},
    dto, system_clock, uuid_v7_millis,
};
use rescrow_server::{
    Server,
    admission::{Lane, Lease},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};

pub struct Api<S> {
    router: Router,
    server: Server<S>,
}
struct Shared<S> {
    server: Server<S>,
    config: Arc<Config>,
    auth: Arc<dyn Authenticator>,
    cursors: Arc<Cursors>,
    auth_slots: [Arc<Semaphore>; 3],
}
impl<S> Clone for Shared<S> {
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
            config: self.config.clone(),
            auth: self.auth.clone(),
            cursors: self.cursors.clone(),
            auth_slots: self.auth_slots.clone(),
        }
    }
}
struct Tracking {
    caller: OnceLock<AccessContext>,
    prepare: OnceLock<dto::Prepare>,
}
struct RequestContext {
    id: String,
    tracking: Arc<Tracking>,
    required: DurabilityProfile,
}
impl<S: TransactionalStore + Readiness + Send + Sync + 'static> Api<S> {
    pub fn new(
        app: Application<S>,
        config: Config,
        auth: Arc<dyn Authenticator>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let server =
            Server::start_with_limits(app, config.required_profile, config.limits.clone())?;
        let auth_slots = [
            config.limits.admission.concurrency.max(1),
            config.limits.decisions.concurrency,
            config.limits.recovery.concurrency,
        ]
        .map(|n| Arc::new(Semaphore::new(n as usize)));
        let shared = Shared {
            server: server.clone(),
            cursors: Arc::new(Cursors::new(&config.cursor_key)?),
            config: Arc::new(config),
            auth,
            auth_slots,
        };
        let router=Router::new()
            .route("/v1/resources",get(dispatch::<S>))
            .route("/v1/resources/{id}",get(dispatch::<S>).put(dispatch::<S>))
            .route("/v1/transactions",get(dispatch::<S>).post(dispatch::<S>))
            .route("/v1/transactions/{id}",get(dispatch::<S>))
            .route("/v1/transactions/{id}/commit",post(dispatch::<S>))
            .route("/v1/transactions/{id}/abort",post(dispatch::<S>))
            .fallback(not_found::<S>)
            .method_not_allowed_fallback(method_not_allowed::<S>)
            .layer(tower_http::trace::TraceLayer::new_for_http().make_span_with(|request:&Request<Body>| {
                tracing::info_span!("http_request",method=%request.method(),route=request.extensions().get::<MatchedPath>().map(MatchedPath::as_str).unwrap_or("unmatched"))
            }))
            .layer(middleware::from_fn_with_state(shared.clone(),boundary::<S>))
            .with_state(shared);
        Ok(Self { router, server })
    }
    pub fn router(&self) -> Router {
        self.router.clone()
    }
    pub fn server(&self) -> &Server<S> {
        &self.server
    }
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let server = self.server.clone();
        let result = axum::serve(listener, self.router)
            .with_graceful_shutdown(async move {
                shutdown.await;
                server.control().close();
            })
            .await;
        self.server.drain().await;
        result
    }
    pub async fn serve_tls(
        self,
        listener: std::net::TcpListener,
        tls: axum_server::tls_rustls::RustlsConfig,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let handle = axum_server::Handle::new();
        let stop = handle.clone();
        let server = self.server.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown.await;
            server.control().close();
            stop.graceful_shutdown(None);
        });
        let result = axum_server::from_tcp_rustls(listener, tls)?
            .handle(handle)
            .serve(self.router.into_make_service())
            .await;
        shutdown_task.abort();
        self.server.drain().await;
        result
    }
}
fn error(code: Code, message: &str) -> Error {
    Error::new(Category::Input, code, message)
}
fn profile_name(profile: DurabilityProfile) -> &'static str {
    match profile {
        DurabilityProfile::Simulation => "simulation",
        DurabilityProfile::Local => "local",
        DurabilityProfile::Regional => "regional",
        DurabilityProfile::EnterpriseMr => "enterprise-mr",
    }
}
fn profile(headers: &HeaderMap, default: DurabilityProfile) -> Result<DurabilityProfile, Error> {
    let values = headers.get_all("require-durability-profile");
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(default);
    };
    if values.next().is_some() {
        return Err(Error::input("durability requirement must occur once"));
    }
    match value
        .to_str()
        .map_err(|_| Error::input("invalid durability requirement"))?
    {
        "simulation" => Ok(DurabilityProfile::Simulation),
        "local" => Ok(DurabilityProfile::Local),
        "regional" => Ok(DurabilityProfile::Regional),
        "enterprise-mr" => Ok(DurabilityProfile::EnterpriseMr),
        _ => Err(Error::input("unknown durability profile")),
    }
}
fn hint(parts: &axum::http::request::Parts) -> Option<String> {
    if parts.method == Method::POST && parts.uri.path() == "/v1/transactions" {
        return parts
            .headers
            .get("idempotency-key")
            .and_then(|h| h.to_str().ok())
            .filter(|id| uuid_v7_millis(id).is_some())
            .map(str::to_owned);
    }
    let segments: Vec<_> = parts.uri.path().split('/').collect();
    if segments.get(2) != Some(&"transactions") {
        return None;
    }
    let id = percent_encoding::percent_decode_str(segments.get(3)?)
        .decode_utf8()
        .ok()?
        .into_owned();
    api::decode_path_id(&id).ok()
}
fn request_id(headers: &HeaderMap) -> Result<String, Error> {
    let mut values = headers.get_all("x-request-id").iter();
    match values.next() {
        None => rescrow_application::new_id(),
        Some(value) => {
            if values.next().is_some() {
                return Err(Error::input("request ID must occur once"));
            }
            let id = value
                .to_str()
                .map_err(|_| Error::input("invalid request ID"))?;
            RecordKey::resource(id)?;
            Ok(id.into())
        }
    }
}
fn request_lane(method: &Method, path: &str) -> Lane {
    if *method == Method::POST && (path.ends_with("/commit") || path.ends_with("/abort")) {
        Lane::Decision
    } else if *method == Method::POST || *method == Method::PUT {
        Lane::Admission
    } else {
        Lane::Recovery
    }
}
fn slot(lane: Lane) -> usize {
    match lane {
        Lane::Admission => 0,
        Lane::Decision => 1,
        Lane::Recovery => 2,
    }
}
fn metadata<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    id: &str,
) -> Metadata {
    Metadata {
        request_id: id.into(),
        serving_region: shared.config.serving_region.clone(),
        deployment_cell: shared.config.deployment_cell.clone(),
        authority_generation: shared.server.authority_generation().into(),
        durability_profile: shared.server.capabilities().durability_profile,
    }
}
fn finish<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    id: &str,
    mut response: Response,
) -> Response {
    let headers = response.headers_mut();
    headers.insert("x-request-id", id.parse().expect("validated request ID"));
    headers.insert(
        "durability-profile",
        profile_name(shared.server.capabilities().durability_profile)
            .parse()
            .unwrap(),
    );
    headers.insert(
        "authority-generation",
        shared
            .server
            .authority_generation()
            .parse()
            .expect("validated authority UUID"),
    );
    response
}
fn problem<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    id: &str,
    mut value: Error,
    tx: Option<String>,
) -> Response {
    if value.transaction_id.is_none() {
        value.transaction_id = tx;
    }
    tracing::debug!(request_id=id,code=?value.code,"request rejected");
    let value = api::Problem::from_error(value, id.into());
    let status = StatusCode::from_u16(value.status).expect("documented problem status");
    let transaction_id = value.transaction_id.clone();
    let retryable = value.retryable;
    let mut response = (status, Json(value)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/problem+json".parse().unwrap(),
    );
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, "Bearer".parse().unwrap());
    }
    if retryable {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().unwrap());
    }
    if let Some(id) = transaction_id
        && RecordKey::resource(&id).is_ok()
    {
        response.headers_mut().insert(
            header::LOCATION,
            format!(
                "/v1/transactions/{}",
                api::path_id(&id).expect("validated transaction identity")
            )
            .parse()
            .unwrap(),
        );
    }
    finish(shared, id, response)
}
fn response<S: TransactionalStore + Readiness + Send + Sync + 'static, T: Serialize>(
    shared: &Shared<S>,
    ctx: &RequestContext,
    status: StatusCode,
    data: T,
    links: BTreeMap<String, String>,
) -> Response {
    let location = links.get("self").cloned();
    let mut value = (
        status,
        Json(Envelope {
            data,
            metadata: metadata(shared, &ctx.id),
            links,
        }),
    )
        .into_response();
    if let Some(location) = location {
        value
            .headers_mut()
            .insert(header::LOCATION, location.parse().expect("validated link"));
    }
    if status == StatusCode::ACCEPTED {
        value
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().unwrap());
    }
    finish(shared, &ctx.id, value)
}
async fn boundary<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    State(shared): State<Shared<S>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let started = Instant::now();
    let (parts, body) = request.into_parts();
    let id = match request_id(&parts.headers) {
        Ok(id) => id,
        Err(e) => return problem(&shared, "request-id-unavailable", e, None),
    };
    if parts.uri.to_string().len() > 4096 {
        return problem(
            &shared,
            &id,
            error(
                Code::LimitExceeded,
                "request URI exceeds its configured bound",
            ),
            None,
        );
    }
    let tx = hint(&parts);
    let tracking = Arc::new(Tracking {
        caller: OnceLock::new(),
        prepare: OnceLock::new(),
    });
    let required = match profile(&parts.headers, shared.config.required_profile) {
        Ok(p) => p,
        Err(e) => return problem(&shared, &id, e, tx),
    };
    let lane = request_lane(&parts.method, parts.uri.path());
    let auth_permit = match shared.auth_slots[slot(lane)].clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return problem(
                &shared,
                &id,
                Error::new(
                    Category::Admission,
                    Code::Throttled,
                    "authentication lane is full; retry the identical request",
                ),
                tx,
            );
        }
    };
    let auth = shared.auth.clone();
    let tracking_inner = tracking.clone();
    let id_inner = id.clone();
    let timeout = Duration::from_millis(shared.config.limits.request_timeout_ms);
    let soft = timeout
        .saturating_sub(Duration::from_millis(100))
        .max(Duration::from_millis(1));
    let operation = async {
        let verified = tokio::task::spawn_blocking(move || {
            let _permit = auth_permit;
            let caller = auth.authenticate(&parts);
            (parts, caller)
        })
        .await;
        let (mut parts, caller) = match verified {
            Ok((parts, Ok(caller))) => (parts, caller),
            Ok((_, Err(e))) => return problem(&shared, &id_inner, e, tx.clone()),
            Err(_) => {
                return problem(
                    &shared,
                    &id_inner,
                    Error::new(
                        Category::Storage,
                        Code::Io,
                        "identity verification is unavailable",
                    ),
                    tx.clone(),
                );
            }
        };
        if !shared.server.capabilities().multi_tenant
            && caller.scope() != &rescrow_application::authority::Scope::local()
        {
            return problem(
                &shared,
                &id_inner,
                error(
                    Code::NotFound,
                    "object is outside the authorized authority scope",
                ),
                tx.clone(),
            );
        }
        if parts
            .headers
            .get_all("require-authority-generation")
            .iter()
            .count()
            > 1
        {
            return problem(
                &shared,
                &id_inner,
                Error::input("authority generation requirement must occur once"),
                tx.clone(),
            );
        }
        if (parts.method == Method::GET || parts.method == Method::HEAD)
            && (parts
                .headers
                .get(header::CONTENT_LENGTH)
                .is_some_and(|v| v != "0")
                || parts.headers.contains_key(header::TRANSFER_ENCODING))
        {
            return problem(
                &shared,
                &id_inner,
                Error::input("read endpoints accept no request body"),
                tx.clone(),
            );
        }
        if let Some(expected) = parts.headers.get("require-authority-generation")
            && expected.to_str().ok() != Some(shared.server.authority_generation())
        {
            return problem(
                &shared,
                &id_inner,
                error(
                    Code::Conflict,
                    "authority generation differs from the required authority",
                ),
                tx.clone(),
            );
        }
        let _ = tracking_inner.caller.set(caller);
        parts.extensions.insert(Arc::new(RequestContext {
            id: id_inner,
            tracking: tracking_inner,
            required,
        }));
        next.run(Request::from_parts(parts, body)).await
    };
    match tokio::time::timeout(soft, operation).await {
        Ok(reply) => reply,
        Err(_) => {
            let remaining = timeout.saturating_sub(started.elapsed());
            if !remaining.is_zero()
                && let Ok(Some(reply)) =
                    tokio::time::timeout(remaining, processing(&shared, &id, &tracking, required))
                        .await
            {
                return reply;
            }
            problem(
                &shared,
                &id,
                Error::new(
                    Category::UnknownOutcome,
                    Code::DeadlineExceeded,
                    "request deadline exceeded",
                ),
                tx,
            )
        }
    }
}
async fn processing<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    id: &str,
    tracking: &Tracking,
    required: DurabilityProfile,
) -> Option<Response> {
    let context = tracking.caller.get()?;
    let request = tracking.prepare.get()?;
    let lease = shared.server.reserve(context, Lane::Recovery).await.ok()?;
    match shared
        .server
        .inspect_preparation(context.clone(), request.clone(), required, lease)
        .await
    {
        Ok(Some(tx)) => {
            let prepared = matches!(tx.state, dto::State::Prepared {});
            let mut data = api::Transaction::from_dto(tx).ok()?;
            data.processing = prepared;
            let links = transaction_links(&data.transaction.request.transaction_id);
            let ctx = RequestContext {
                id: id.into(),
                tracking: Arc::new(Tracking {
                    caller: OnceLock::new(),
                    prepare: OnceLock::new(),
                }),
                required,
            };
            Some(response(
                shared,
                &ctx,
                if prepared {
                    StatusCode::ACCEPTED
                } else {
                    StatusCode::OK
                },
                data,
                links,
            ))
        }
        Ok(None) => None,
        Err(error)
            if matches!(
                error.code,
                Code::Capacity
                    | Code::Conflict
                    | Code::Expired
                    | Code::Overflow
                    | Code::NotFound
                    | Code::Forbidden
                    | Code::Unauthenticated
            ) =>
        {
            Some(problem(
                shared,
                id,
                error,
                Some(request.transaction_id.clone()),
            ))
        }
        Err(_) => None,
    }
}
fn transaction_links(id: &str) -> BTreeMap<String, String> {
    let id = api::path_id(id).expect("validated transaction identity");
    BTreeMap::from([
        ("self".into(), format!("/v1/transactions/{id}")),
        ("commit".into(), format!("/v1/transactions/{id}/commit")),
        ("abort".into(), format!("/v1/transactions/{id}/abort")),
    ])
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    prefix: Option<String>,
    state: Option<String>,
}
fn query(uri: &axum::http::Uri) -> Result<ListQuery, Error> {
    Query::<ListQuery>::try_from_uri(uri)
        .map(|q| q.0)
        .map_err(|_| Error::input("invalid, duplicate, or unknown list query parameter"))
}
fn no_query(uri: &axum::http::Uri) -> Result<(), Error> {
    if uri.query().is_some() {
        Err(Error::input("this endpoint accepts no query parameters"))
    } else {
        Ok(())
    }
}
fn object_id(path: &str) -> Result<String, Error> {
    let segment = path
        .split('/')
        .nth(3)
        .ok_or_else(|| Error::input("missing object ID"))?;
    let id = percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .map_err(|_| Error::input("object ID must be valid UTF-8"))?
        .into_owned();
    api::decode_path_id(&id)
}
fn json_content(headers: &HeaderMap, optional: bool) -> Result<(), Error> {
    match headers.get(header::CONTENT_TYPE) {
        None if optional => Ok(()),
        Some(value)
            if value
                .to_str()
                .ok()
                .and_then(|v| v.split(';').next())
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json")) =>
        {
            Ok(())
        }
        _ => Err(error(
            Code::UnsupportedMediaType,
            "Content-Type must be application/json",
        )),
    }
}
async fn body<T: serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: Body,
    limit: usize,
    empty: bool,
) -> Result<T, Error> {
    json_content(headers, empty)?;
    let bytes = to_bytes(body, limit).await.map_err(|_| {
        error(
            Code::LimitExceeded,
            "request body exceeds its byte limit or could not be read",
        )
    })?;
    canonical::decode(if empty && bytes.is_empty() {
        b"{}"
    } else {
        &bytes
    })
}
fn idempotency(headers: &HeaderMap) -> Result<String, Error> {
    let mut values = headers.get_all("idempotency-key").iter();
    let value = values
        .next()
        .ok_or_else(|| Error::input("Idempotency-Key is required"))?;
    if values.next().is_some() {
        return Err(Error::input("Idempotency-Key must occur once"));
    }
    let id = value
        .to_str()
        .map_err(|_| Error::input("Idempotency-Key must be a canonical UUIDv7"))?;
    if uuid_v7_millis(id).is_none() {
        return Err(Error::input(
            "Idempotency-Key must be a canonical lowercase UUIDv7",
        ));
    }
    Ok(id.into())
}
async fn execute<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    ctx: &RequestContext,
    command: dto::Command,
    lease: Lease,
) -> Result<rescrow_application::transactional::Execution, Error> {
    let caller = ctx
        .tracking
        .caller
        .get()
        .expect("authenticated request context");
    shared
        .server
        .execute_authorized(caller.clone(), command, ctx.required, lease)
        .await
}
async fn dispatch<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    State(shared): State<Shared<S>>,
    Extension(ctx): Extension<Arc<RequestContext>>,
    request: Request<Body>,
) -> Response {
    let (parts, input) = request.into_parts();
    let tx = hint(&parts);
    match dispatch_inner(&shared, &ctx, parts, input).await {
        Ok(response) => response,
        Err(e) => problem(&shared, &ctx.id, e, tx),
    }
}
async fn dispatch_inner<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    ctx: &RequestContext,
    parts: axum::http::request::Parts,
    input: Body,
) -> Result<Response, Error> {
    let caller = ctx
        .tracking
        .caller
        .get()
        .expect("authenticated request context");
    let route = parts
        .extensions
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("");
    let method = if parts.method == Method::HEAD {
        Method::GET
    } else {
        parts.method.clone()
    };
    let action = match (&method, route) {
        (m, "/v1/resources/{id}") if *m == Method::PUT => Action::Create,
        (m, "/v1/transactions") if *m == Method::POST => Action::Prepare,
        (_, "/v1/transactions/{id}/commit") => Action::Commit,
        (_, "/v1/transactions/{id}/abort") => Action::Abort,
        _ => Action::Read,
    };
    caller.require(action)?;
    let lane = request_lane(&method, route);
    let lease = shared.server.reserve(caller, lane).await?;
    let from = system_clock();
    match (method, route) {
        (Method::PUT, "/v1/resources/{id}") => {
            no_query(&parts.uri)?;
            let id = object_id(parts.uri.path())?;
            caller.resource(Action::Create, &id, None)?;
            let value: api::ResourceCreate = body(
                &parts.headers,
                input,
                shared.config.limits.body_bytes,
                false,
            )
            .await?;
            let spec = value.into_spec(id.clone());
            shared.config.limits.resource(&spec)?;
            let result = execute(shared, ctx, dto::Command::CreateResource(spec), lease).await?;
            let dto::Output::Resource(resource) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                if result.replayed {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                },
                api::Resource::observed(resource, from, system_clock().max(from)),
                BTreeMap::from([(
                    "self".into(),
                    format!(
                        "/v1/resources/{}",
                        api::path_id(&id).expect("validated resource identity")
                    ),
                )]),
            ))
        }
        (Method::GET, "/v1/resources/{id}") => {
            no_query(&parts.uri)?;
            let id = object_id(parts.uri.path())?;
            let result = execute(
                shared,
                ctx,
                dto::Command::ShowResource {
                    resource_id: id.clone(),
                },
                lease,
            )
            .await?;
            let dto::Output::Resource(resource) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                StatusCode::OK,
                api::Resource::observed(resource, from, system_clock().max(from)),
                BTreeMap::from([(
                    "self".into(),
                    format!(
                        "/v1/resources/{}",
                        api::path_id(&id).expect("validated resource identity")
                    ),
                )]),
            ))
        }
        (Method::POST, "/v1/transactions") => {
            no_query(&parts.uri)?;
            let id = idempotency(&parts.headers)?;
            let value: api::Prepare = body(
                &parts.headers,
                input,
                shared.config.limits.body_bytes,
                false,
            )
            .await?;
            let request = value.into_command(id.clone());
            shared.config.limits.prepare(&request)?;
            let _ = ctx.tracking.prepare.set(request.clone());
            let result = execute(shared, ctx, dto::Command::Prepare(request), lease).await?;
            let dto::Output::Transaction(tx) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                if result.replayed {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                },
                api::Transaction::from_dto(tx)?,
                transaction_links(&id),
            ))
        }
        (Method::GET, "/v1/transactions/{id}") => {
            no_query(&parts.uri)?;
            let id = object_id(parts.uri.path())?;
            let result = execute(
                shared,
                ctx,
                dto::Command::ShowTransaction {
                    transaction_id: id.clone(),
                },
                lease,
            )
            .await?;
            let dto::Output::Transaction(tx) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                StatusCode::OK,
                api::Transaction::from_dto(tx)?,
                transaction_links(&id),
            ))
        }
        (Method::POST, "/v1/transactions/{id}/commit") => {
            no_query(&parts.uri)?;
            let id = object_id(parts.uri.path())?;
            let value: api::Commit =
                body(&parts.headers, input, shared.config.limits.body_bytes, true).await?;
            shared.config.limits.actuals(value.actuals.as_deref())?;
            let result = execute(
                shared,
                ctx,
                dto::Command::Commit(dto::Commit {
                    transaction_id: id.clone(),
                    actuals: value.actuals,
                }),
                lease,
            )
            .await?;
            let dto::Output::Transaction(tx) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                StatusCode::OK,
                api::Transaction::from_dto(tx)?,
                transaction_links(&id),
            ))
        }
        (Method::POST, "/v1/transactions/{id}/abort") => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Empty {}
            no_query(&parts.uri)?;
            let id = object_id(parts.uri.path())?;
            let _: Empty =
                body(&parts.headers, input, shared.config.limits.body_bytes, true).await?;
            let result = execute(
                shared,
                ctx,
                dto::Command::Abort {
                    transaction_id: id.clone(),
                },
                lease,
            )
            .await?;
            let dto::Output::Transaction(tx) = result.output else {
                unreachable!()
            };
            Ok(response(
                shared,
                ctx,
                StatusCode::OK,
                api::Transaction::from_dto(tx)?,
                transaction_links(&id),
            ))
        }
        (Method::GET, "/v1/resources") => {
            let query = query(&parts.uri)?;
            if query.state.is_some() {
                return Err(Error::input("state filter applies only to transactions"));
            }
            let plan = page_plan(shared, ctx, "resources", &query)?;
            let result = execute(shared, ctx, dto::Command::ListResources, lease).await?;
            let dto::Output::Resources(resources) = result.output else {
                unreachable!()
            };
            let until = system_clock().max(from);
            let items = resources
                .into_iter()
                .map(|r| {
                    (
                        r.spec.resource_id.clone(),
                        api::Resource::observed(r, from, until),
                    )
                })
                .collect();
            let data = page(shared, plan, items)?;
            let links = page_links("resources", &query, data.next_cursor.as_deref());
            Ok(response(shared, ctx, StatusCode::OK, data, links))
        }
        (Method::GET, "/v1/transactions") => {
            let query = query(&parts.uri)?;
            if query
                .state
                .as_deref()
                .is_some_and(|s| !matches!(s, "prepared" | "committed" | "aborted"))
            {
                return Err(Error::input("unknown transaction state filter"));
            }
            let plan = page_plan(shared, ctx, "transactions", &query)?;
            let result = execute(shared, ctx, dto::Command::ListTransactions, lease).await?;
            let dto::Output::Transactions(transactions) = result.output else {
                unreachable!()
            };
            let mut items = Vec::new();
            for tx in transactions {
                let state = match tx.state {
                    dto::State::Prepared {} => "prepared",
                    dto::State::Committed { .. } => "committed",
                    dto::State::Aborted { .. } => "aborted",
                };
                if query.state.as_deref().is_none_or(|wanted| wanted == state) {
                    items.push((tx.request.transaction_id.clone(), tx));
                }
            }
            let selected = page(shared, plan, items)?;
            let data = Page {
                items: selected
                    .items
                    .into_iter()
                    .map(api::Transaction::from_dto)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: selected.next_cursor,
            };
            let links = page_links("transactions", &query, data.next_cursor.as_deref());
            Ok(response(shared, ctx, StatusCode::OK, data, links))
        }
        _ => Err(error(
            Code::MethodNotAllowed,
            "method is not supported for this endpoint",
        )),
    }
}
struct PagePlan {
    limit: usize,
    prefix: String,
    scope: String,
    after: Option<String>,
    now: u64,
}
fn page_plan<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    shared: &Shared<S>,
    ctx: &RequestContext,
    endpoint: &str,
    query: &ListQuery,
) -> Result<PagePlan, Error> {
    let limit = query
        .limit
        .unwrap_or(shared.config.limits.page_size.min(100));
    if limit == 0 || limit > shared.config.limits.page_size {
        return Err(error(
            Code::LimitExceeded,
            "page size is outside the configured limit",
        ));
    }
    let prefix = query.prefix.clone().unwrap_or_default();
    if !prefix.is_empty() {
        RecordKey::resource(&prefix)?;
    }
    let caller = ctx.tracking.caller.get().unwrap();
    let binding = serde_json::to_vec(&(
        shared.server.authority_generation(),
        caller.scope().tenant().as_str(),
        caller.scope().namespace(),
        caller.facts().policy_digest(),
        endpoint,
        &prefix,
        &query.state,
    ))
    .expect("query binding is serializable");
    let scope = canonical::digest("rescrow.pagination.v1", &binding);
    let now = system_clock();
    let after = query
        .cursor
        .as_deref()
        .map(|cursor| shared.cursors.decode(cursor, &scope, now))
        .transpose()?;
    Ok(PagePlan {
        limit,
        prefix,
        scope,
        after,
        now,
    })
}
fn page<S: TransactionalStore + Readiness + Send + Sync + 'static, T>(
    shared: &Shared<S>,
    plan: PagePlan,
    mut items: Vec<(String, T)>,
) -> Result<Page<T>, Error> {
    let PagePlan {
        limit,
        prefix,
        scope,
        after,
        now,
    } = plan;
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut selected: Vec<_> = items
        .into_iter()
        .filter(|(id, _)| id.starts_with(&prefix) && after.as_ref().is_none_or(|a| id > a))
        .take(limit + 1)
        .collect();
    let more = selected.len() > limit;
    if more {
        selected.pop();
    }
    let next_cursor = if more {
        Some(shared.cursors.encode(
            scope,
            selected.last().expect("positive page size").0.clone(),
            now,
        )?)
    } else {
        None
    };
    Ok(Page {
        items: selected.into_iter().map(|(_, value)| value).collect(),
        next_cursor,
    })
}
fn page_links(endpoint: &str, query: &ListQuery, cursor: Option<&str>) -> BTreeMap<String, String> {
    let mut links = BTreeMap::from([("self".into(), format!("/v1/{endpoint}"))]);
    if let Some(cursor) = cursor {
        let mut next = format!("/v1/{endpoint}?cursor={cursor}");
        if let Some(limit) = query.limit {
            next.push_str(&format!("&limit={limit}"));
        }
        if let Some(prefix) = &query.prefix {
            next.push_str(&format!("&prefix={prefix}"));
        }
        if let Some(state) = &query.state {
            next.push_str(&format!("&state={state}"));
        }
        links.insert("next".into(), next);
    }
    links
}
async fn not_found<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    State(shared): State<Shared<S>>,
    Extension(ctx): Extension<Arc<RequestContext>>,
) -> Response {
    problem(
        &shared,
        &ctx.id,
        error(Code::NotFound, "endpoint does not exist"),
        None,
    )
}
async fn method_not_allowed<S: TransactionalStore + Readiness + Send + Sync + 'static>(
    State(shared): State<Shared<S>>,
    Extension(ctx): Extension<Arc<RequestContext>>,
) -> Response {
    problem(
        &shared,
        &ctx.id,
        error(
            Code::MethodNotAllowed,
            "method is not supported for this endpoint",
        ),
        None,
    )
}
