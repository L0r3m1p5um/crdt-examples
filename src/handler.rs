use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::crdt;
use crate::crdt::StateCRDT;
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};
pub enum WorkerOperation<T, V, O> {
    Merge(T),
    ReadValue(oneshot::Sender<V>),
    Update(O),
}

pub struct CRDTWorker<T, V, O>
where
    T: crdt::StateCRDT<V, O>,
{
    crdt: Arc<RwLock<T>>,
    tx: mpsc::Sender<WorkerOperation<T, V, O>>,
    rx: mpsc::Receiver<WorkerOperation<T, V, O>>,
    senders: Vec<mpsc::Sender<WorkerOperation<T, V, O>>>,
}

impl<T, V, O> CRDTWorker<T, V, O>
where
    T: crdt::StateCRDT<V, O> + Send + Sync + Clone,
    O: Send + Sync,
    V: Send + Sync + 'static,
{
    fn new(crdt: T) -> Self {
        let (tx, rx) = mpsc::channel(512);
        CRDTWorker {
            crdt: Arc::new(RwLock::new(crdt)),
            tx,
            rx,
            senders: vec![],
        }
    }

    fn register_worker(&mut self, tx: mpsc::Sender<WorkerOperation<T, V, O>>) {
        self.senders.push(tx);
    }

    async fn start_worker<'a>(&'a mut self) {
        let senders = self.senders.clone();
        let values = self.crdt.clone();
        tokio_scoped::scope(|scope| {
            let _ = scope.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    for tx in senders.iter() {
                        let copy: T = values.read().unwrap().clone();
                        let merge = WorkerOperation::Merge(copy);
                        tx.send(merge).await.unwrap();
                    }
                }
            });
            let _ = scope.spawn(async move {
                while let Some(op) = self.rx.recv().await {
                    match self.handle_operation(op) {
                        _ => (),
                    }
                }
            });
        });
    }

    fn value(&self) -> V {
        self.crdt.read().unwrap().value()
    }

    pub fn sender(&self) -> mpsc::Sender<WorkerOperation<T, V, O>> {
        self.tx.clone()
    }

    fn merge(&mut self, other: &T) {
        self.crdt.write().unwrap().merge(other);
    }

    fn update(&mut self, operation: O) {
        self.crdt.write().unwrap().update(operation);
    }

    fn handle_operation(&mut self, op: WorkerOperation<T, V, O>) -> Result<(), V> {
        match op {
            WorkerOperation::Merge(value) => Ok(self.merge(&value)),
            WorkerOperation::ReadValue(tx) => tx.send(self.value()),
            WorkerOperation::Update(op) => Ok(self.update(op)),
        }
    }
}

pub enum HandlerOperation<V, O> {
    Update(O),
    ReadValue(oneshot::Sender<V>),
}

pub struct CRDTHandler<T, V, O> {
    tx: mpsc::Sender<HandlerOperation<V, O>>,
    rx: mpsc::Receiver<HandlerOperation<V, O>>,
    crdt_init: Vec<T>,
    senders: Vec<mpsc::Sender<WorkerOperation<T, V, O>>>,
}

impl<T, V, O> CRDTHandler<T, V, O>
where
    T: StateCRDT<V, O>,
    O: Send + Sync,
    V: Send + Sync + 'static,
{
    pub fn new(crdt_init: Vec<T>) -> Self {
        let (tx, rx) = mpsc::channel(512);
        Self {
            tx,
            rx,
            crdt_init,
            senders: vec![],
        }
    }

    pub fn sender(&self) -> Sender<HandlerOperation<V, O>> {
        self.tx.clone()
    }

    pub async fn start_process(&mut self) {
        tokio_scoped::scope(|scope| {
            let mut workers: Vec<CRDTWorker<T, V, O>> = self
                .crdt_init
                .clone()
                .into_iter()
                .map(|value| CRDTWorker::new(value))
                .collect();
            self.senders = workers.iter().map(|worker| worker.sender()).collect();
            for sender in &self.senders {
                for worker in &mut workers {
                    worker.register_worker(sender.clone())
                }
            }

            let mut handles = vec![];

            workers.into_iter().for_each(|mut worker| {
                let _ = scope.spawn(async move {
                    worker.start_worker().await;
                });
            });

            handles.push(scope.spawn(async move {
                while let Some(op) = self.rx.recv().await {
                    match self.handle_operation(op).await {
                        _ => (),
                    }
                }
            }));
        });
    }

    fn get_sender(&mut self) -> Sender<WorkerOperation<T, V, O>> {
        let index = fastrand::usize(0..self.senders.len());
        let sender = self.senders[index].clone();
        sender
    }

    async fn handle_operation(&mut self, op: HandlerOperation<V, O>) -> Result<(), ()> {
        match op {
            HandlerOperation::Update(x) => {
                self.get_sender()
                    .send(WorkerOperation::Update(x))
                    .await
                    .unwrap();
                Ok(())
            }
            HandlerOperation::ReadValue(rx) => {
                let (wtx, wrx) = oneshot::channel();
                self.get_sender()
                    .send(WorkerOperation::ReadValue(wtx))
                    .await
                    .unwrap();
                let value = wrx.await.map_err(|_| ())?;
                let result = rx.send(value);
                if result.is_err() {
                    tracing::error!("Could not send back count value");
                }
                result.map_err(|_| ())?;
                Ok(())
            }
        }
    }
}
