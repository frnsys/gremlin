//! Task pool, for background jobs.

use std::{
    marker::PhantomData,
    sync::{mpsc, Arc, Mutex},
    thread,
};

use arc_swap::ArcSwapOption;

type JobOutput<T> = Arc<ArcSwapOption<T>>;

pub struct JobHandle<T>(JobOutput<T>);
impl<T> JobHandle<T> {
    pub fn ready(&self) -> bool {
        self.0.load().is_some()
    }

    pub fn poll(&self) -> Option<T> {
        let ready = self.0.load().is_some();
        if ready {
            let val = self
                .0
                .swap(None)
                .expect("We checked that the result is Some");
            Some(
                Arc::into_inner(val)
                    .expect("Only one handle should be left"),
            )
        } else {
            None
        }
    }
}

enum Message<T> {
    NewJob(JobOutput<T>, Job<T>),
    Terminate,
}

pub struct ThreadPool<T: Send + Sync + 'static> {
    workers: Vec<Worker<T>>,
    sender: mpsc::Sender<Message<T>>,
}

pub trait FnBox<T> {
    fn call_box(self: Box<Self>) -> T;
}

impl<T, F: FnOnce() -> T> FnBox<T> for F {
    fn call_box(self: Box<F>) -> T {
        (*self)()
    }
}

type Job<T> = Box<dyn FnBox<T> + Send + 'static>;

impl<T: Send + Sync + 'static> ThreadPool<T> {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool<T> {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F) -> JobHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let job = Box::new(f);
        let output = Arc::new(ArcSwapOption::from(None));
        self.sender
            .send(Message::<T>::NewJob(output.clone(), job))
            .unwrap();

        JobHandle(output)
    }

    fn terminate_all(&mut self) {
        for _ in &mut self.workers {
            self.sender.send(Message::<T>::Terminate).unwrap();
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    pub fn reset(&mut self) {
        let n = self.workers.len();
        *self = ThreadPool::new(n);
    }
}

impl<T: Send + Sync + 'static> Drop for ThreadPool<T> {
    fn drop(&mut self) {
        self.terminate_all();
    }
}

pub fn task<F, T: Send + Sync + 'static>(f: F) -> JobHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
{
    let output = Arc::new(ArcSwapOption::from(None));
    let t_output = output.clone();
    thread::spawn(move || {
        let result = f();
        t_output.store(Some(Arc::new(result)));
    });
    JobHandle(output)
}

struct Worker<T: Send + Sync + 'static> {
    thread: Option<thread::JoinHandle<()>>,
    marker: PhantomData<T>,
}

impl<T: Send + Sync + 'static> Worker<T> {
    fn new(
        receiver: Arc<Mutex<mpsc::Receiver<Message<T>>>>,
    ) -> Worker<T> {
        let thread = thread::spawn(move || loop {
            let message =
                receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(output, job) => {
                    let result = job.call_box();
                    output.store(Some(Arc::new(result)));
                }
                Message::Terminate => {
                    break;
                }
            }
        });

        Worker::<T> {
            marker: PhantomData,
            thread: Some(thread),
        }
    }
}
