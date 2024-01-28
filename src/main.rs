mod crdt;
mod handler;

use std::collections::HashSet;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};

use crdt::{GCounter, StateCRDT, TwoPSetOperation};
use handler::{CRDTHandler, HandlerOperation};
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gcounter=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let size = 5;
    let increment_tx = handler_init(size, |i, s| GCounter::new(i, s)).await;
    let max_tx = handler_init(size, |_, _| crdt::Max::new()).await;
    let pncounter_tx = handler_init(size, |i, s| crdt::PNCounter::new(i, s)).await;
    let gset_tx = handler_init(size, |_, _| crdt::GSet::new()).await;
    let twopset_tx = handler_init(size, |_, _| crdt::TwoPSet::new()).await;

    let app = Router::new()
        .route("/", get(root))
        .route("/increment", post(increment))
        .route("/count", get(read_count))
        .route("/max", post(set_max))
        .route("/max", get(get_max))
        .route("/pncounter", post(update_pncounter))
        .route("/pncounter", get(read_pncounter))
        .route("/gset", post(update_gset))
        .route("/gset", get(read_gset))
        .route("/twopset", get(read_twopset))
        .route("/twopset/add", post(add_twopset))
        .route("/twopset/remove", post(remove_twopset))
        .with_state(AppState {
            increment_tx,
            max_tx,
            pncounter_tx,
            gset_tx,
            twopset_tx,
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

#[derive(Clone)]
struct AppState {
    increment_tx: mpsc::Sender<HandlerOperation<usize, usize>>,
    max_tx: mpsc::Sender<HandlerOperation<u64, u64>>,
    pncounter_tx: mpsc::Sender<HandlerOperation<isize, isize>>,
    gset_tx: mpsc::Sender<HandlerOperation<HashSet<String>, String>>,
    twopset_tx: mpsc::Sender<HandlerOperation<HashSet<i32>, TwoPSetOperation<i32>>>,
}

async fn handler_init<F, T, V, O>(size: usize, init_fn: F) -> Sender<HandlerOperation<V, O>>
where
    F: Fn(usize, usize) -> T,
    T: StateCRDT<V, O> + 'static,
    V: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    let state = (0..size - 1)
        .into_iter()
        .map(|index| init_fn(index, size))
        .collect();
    start_handler(state).await
}
async fn start_handler<T, V, O>(init_state: Vec<T>) -> Sender<HandlerOperation<V, O>>
where
    T: Send + Sync + StateCRDT<V, O> + 'static,
    V: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    let mut handler = CRDTHandler::new(init_state);
    let tx = handler.sender();
    tokio::spawn(async move {
        handler.start_process().await;
    });
    tx
}

async fn root() -> &'static str {
    "Hello, World!"
}

#[derive(Serialize, Deserialize)]
struct MaxValue {
    value: u64,
}

#[derive(Serialize, Deserialize)]
struct ValueResponse<T> {
    value: T,
}

async fn set_max(State(state): State<AppState>, Json(req): Json<MaxValue>) -> StatusCode {
    send_update(&state.max_tx, req.value).await
}

async fn get_max(State(state): State<AppState>) -> Result<Json<ValueResponse<u64>>, StatusCode> {
    send_read(&state.max_tx).await
}

async fn update_pncounter(
    State(state): State<AppState>,
    Json(req): Json<ValueResponse<isize>>,
) -> StatusCode {
    send_update(&state.pncounter_tx, req.value).await
}

async fn read_pncounter(
    State(state): State<AppState>,
) -> Result<Json<ValueResponse<isize>>, StatusCode> {
    send_read(&state.pncounter_tx).await
}

async fn update_gset(
    State(state): State<AppState>,
    Json(req): Json<ValueResponse<String>>,
) -> StatusCode {
    send_update(&state.gset_tx, req.value).await
}

async fn read_gset(
    State(state): State<AppState>,
) -> Result<Json<ValueResponse<HashSet<String>>>, StatusCode> {
    send_read(&state.gset_tx).await
}

async fn add_twopset(
    State(state): State<AppState>,
    Json(req): Json<ValueResponse<i32>>,
) -> StatusCode {
    send_update(&state.twopset_tx, TwoPSetOperation::Add(req.value)).await
}

async fn remove_twopset(
    State(state): State<AppState>,
    Json(req): Json<ValueResponse<i32>>,
) -> StatusCode {
    send_update(&state.twopset_tx, TwoPSetOperation::Remove(req.value)).await
}

async fn read_twopset(
    State(state): State<AppState>,
) -> Result<Json<ValueResponse<HashSet<i32>>>, StatusCode> {
    send_read(&state.twopset_tx).await
}

async fn increment(State(state): State<AppState>) -> StatusCode {
    send_update(&state.increment_tx, 1).await
}

async fn read_count(
    State(state): State<AppState>,
) -> Result<Json<ValueResponse<usize>>, StatusCode> {
    send_read(&state.increment_tx).await
}

async fn send_read<V, O>(
    tx: &Sender<HandlerOperation<V, O>>,
) -> Result<Json<ValueResponse<V>>, StatusCode> {
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = tx
        .send(HandlerOperation::ReadValue(resp_tx))
        .await
        .map_err(|e| {
            tracing::error!("Failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let value = resp_rx.await.map_err(|e| {
        tracing::error!("Couldn't receive value: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(ValueResponse { value }))
}

async fn send_update<V, O>(tx: &Sender<HandlerOperation<V, O>>, op: O) -> StatusCode {
    let result = tx.send(HandlerOperation::Update(op)).await;
    match result {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            tracing::error!("Could not increment: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Serialize)]
struct Response {
    message: &'static str,
}

#[derive(Serialize)]
struct CountResponse {
    count: usize,
}
#[cfg(test)]
mod test {}
