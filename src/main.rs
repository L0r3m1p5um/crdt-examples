use std::sync::{Arc, RwLock};
use std::{cmp::max, time::Duration};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};

use serde::Serialize;
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};
use tokio::time::Instant;
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
    let mut handler = CountHandler::new(3);
    let tx = handler.sender();

    tokio::spawn(async move {
        handler.start_process().await;
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/increment", post(increment))
        .route("/count", get(read_count))
        .with_state(IncrementState { tx: tx });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Hello, World!"
}

async fn increment(State(state): State<IncrementState>) -> StatusCode {
    let start = Instant::now();
    let result = state.tx.send(CountOperation::Increment).await;
    tracing::debug!("Elapsed: {}", start.elapsed().as_nanos());
    match result {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            tracing::error!("Could not increment: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn read_count(
    State(state): State<IncrementState>,
) -> Result<Json<CountResponse>, StatusCode> {
    let (tx, rx) = oneshot::channel();
    state
        .tx
        .send(CountOperation::ReadValue(tx))
        .await
        .map_err(|e| {
            tracing::error!("Couldn't send tx: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let (count, gcount) = rx.await.map_err(|e| {
        tracing::error!("Couldn't read count: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(CountResponse { count, gcount }))
}

#[derive(Serialize)]
struct Response {
    message: &'static str,
}

#[derive(Clone)]
struct IncrementState {
    tx: mpsc::Sender<CountOperation>,
}

struct CountHandler {
    tx: mpsc::Sender<CountOperation>,
    rx: mpsc::Receiver<CountOperation>,
    count: usize,
    workers: usize,
    senders: Vec<mpsc::Sender<WorkerOperation>>,
}

impl CountHandler {
    fn new(workers: usize) -> Self {
        let (tx, rx) = mpsc::channel(512);
        Self {
            count: 0,
            tx,
            rx,
            workers,
            senders: vec![],
        }
    }

    fn sender(&self) -> Sender<CountOperation> {
        self.tx.clone()
    }

    fn increment(&mut self) {
        self.count += 1;
    }

    async fn start_process(&mut self) {
        let mut workers: Vec<CountWorker> = (0..self.workers)
            .into_iter()
            .map(|index| CountWorker::new(self.workers, index))
            .collect();
        self.senders = workers.iter().map(|worker| worker.sender()).collect();
        for sender in &self.senders {
            for worker in &mut workers {
                worker.register_worker(sender.clone()).await
            }
        }

        workers.into_iter().for_each(|mut worker| {
            tokio::spawn(async move {
                worker.start_worker().await;
            });
        });

        while let Some(op) = self.rx.recv().await {
            match self.handle_operation(op).await {
                _ => (),
            }
        }
    }

    fn get_sender(&mut self) -> Sender<WorkerOperation> {
        let index = fastrand::usize(0..self.senders.len());
        let sender = self.senders[index].clone();
        sender
    }

    async fn handle_operation(&mut self, op: CountOperation) -> Result<(), ()> {
        match op {
            CountOperation::Increment => {
                self.get_sender()
                    .send(WorkerOperation::Increment)
                    .await
                    .unwrap();
                Ok(self.increment())
            }
            CountOperation::ReadValue(rx) => {
                let (wtx, wrx) = oneshot::channel();
                self.get_sender()
                    .send(WorkerOperation::ReadValue(wtx))
                    .await
                    .unwrap();
                let gcount = wrx.await.map_err(|_| ())?;
                let result = rx.send((self.count, gcount));
                if result.is_err() {
                    tracing::error!("Could not send back count value");
                }
                result.map_err(|_| ())?;
                Ok(())
            }
        }
    }
}

enum CountOperation {
    Increment,
    ReadValue(oneshot::Sender<(usize, usize)>),
}

#[derive(Debug)]
enum WorkerOperation {
    Increment,
    ReadValue(oneshot::Sender<usize>),
    Merge(Vec<usize>),
}

#[derive(Serialize)]
struct CountResponse {
    count: usize,
    gcount: usize,
}

struct CountWorker {
    values: Arc<RwLock<Vec<usize>>>,
    index: usize,
    tx: mpsc::Sender<WorkerOperation>,
    rx: mpsc::Receiver<WorkerOperation>,
    senders: Vec<mpsc::Sender<WorkerOperation>>,
}

impl CountWorker {
    fn new(size: usize, index: usize) -> Self {
        let (tx, rx) = mpsc::channel(512);
        CountWorker {
            values: Arc::new(RwLock::new(vec![0; size])),
            index,
            tx,
            rx,
            senders: vec![],
        }
    }

    async fn register_worker(&mut self, tx: mpsc::Sender<WorkerOperation>) {
        self.senders.push(tx);
    }

    async fn start_worker(&mut self) {
        let senders = self.senders.clone();
        let values = self.values.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                for tx in senders.iter() {
                    let merge = WorkerOperation::Merge(values.read().unwrap().clone());
                    tracing::debug!("State: {:?}", merge);
                    tx.send(merge).await.unwrap();
                }
            }
        });
        while let Some(op) = self.rx.recv().await {
            match self.handle_operation(op) {
                _ => (),
            }
        }
    }

    fn value(&self) -> usize {
        self.values.read().unwrap().iter().sum()
    }

    fn sender(&self) -> mpsc::Sender<WorkerOperation> {
        self.tx.clone()
    }

    fn increment(&mut self) {
        self.values.write().unwrap()[self.index] += 1;
    }

    fn merge(&mut self, vals: Vec<usize>) {
        let new_values: Vec<usize> = self
            .values
            .read()
            .unwrap()
            .iter()
            .zip(vals)
            .map(|(first, second)| max(*first, second))
            .collect();
        let mut lock = self.values.write().unwrap();
        *lock = new_values;
    }

    fn handle_operation(&mut self, op: WorkerOperation) -> Result<(), usize> {
        match op {
            WorkerOperation::Increment => Ok(self.increment()),
            WorkerOperation::ReadValue(tx) => tx.send(self.value()),
            WorkerOperation::Merge(vals) => Ok(self.merge(vals)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::CountWorker;

    #[test]
    fn increment_worker() {
        let mut worker = CountWorker::new(3, 1);
        *worker.values.write().unwrap() = vec![1, 0, 3];
        worker.increment();
        assert!(*worker.values.read().unwrap() == vec![1, 1, 3]);
    }

    #[test]
    fn merge_worker() {
        let mut worker = CountWorker::new(3, 1);
        *worker.values.write().unwrap() = vec![0, 1, 3];
        worker.merge(vec![2, 1, 2]);
        assert!(*worker.values.read().unwrap() == vec![2, 1, 3]);
    }
}
