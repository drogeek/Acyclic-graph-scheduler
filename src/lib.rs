use petgraph::Graph;
use std::sync::Arc;

//todo use Arg<T> intsead of Vec<T>
pub type Job<T> = fn(Vec<T>) -> T;
pub type ExecutableGraphType<T> = Graph<Job<T>, Option<T>>;

pub mod thread {
    use std::thread::JoinHandle;
    use petgraph::graph::NodeIndex;
    use std::sync::{Arc, Mutex, mpsc};
    use std::sync::mpsc::{Sender, Receiver};
    use std::thread;
    use petgraph::Graph;
    use crate::{Job, ExecutableGraphType};

    #[derive(Debug)]
    pub struct Result<T> {
        pub node_id: NodeIndex,
        pub value: T,
    }

    enum Message<T> {
        Operation(Operation<T>),
        Terminate
    }

    pub struct Operation<T>{
        pub node_id: NodeIndex,
        pub name: String,
        pub data: Vec<T>,
        pub f: Job<T>
    }
    impl<T> Operation<T>{
        pub fn new(node_id: NodeIndex, data: Vec<T>, name: String, g: &ExecutableGraphType<T>) -> Operation<T>{
            Operation{
                node_id,
                data,
                name,
                f: g[node_id]
            }
        }
    }

    struct Worker {
        thread: Option<JoinHandle<()>>
    }

    impl Worker {
        fn new<T: Send + 'static>(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message<T>>>>, sender: Sender<Result<T>>)
                                  -> Worker {
            let thread = thread::spawn(move || loop {
                let receiver = Arc::clone(&receiver);
                let msg = receiver.lock().unwrap().recv().unwrap();
                match msg {
                    Message::Operation(Operation{node_id, data, name, f}) => {
                        println!("{} ({:?}) is being executed  by Worker {}", name, node_id, id);
                        sender.send(Result {
                            node_id,
                            value: f(data)
                        });
                    }
                    Message::Terminate => break
                }

            });
            Worker {
                thread: Some(thread),
            }

        }
    }

    pub struct ThreadPool<T: Send> {
        workers: Vec<Worker>,
        receiver_thread_result: Receiver<Result<T>>,
        sender: Sender<Message<T>>
    }

    impl<T: Send> Drop for ThreadPool<T>{
        fn drop(&mut self) {
            for _ in &self.workers {
                self.sender.send(Message::Terminate).unwrap();
            }

            for worker in &mut self.workers {
                if let Some(thread) = worker.thread.take(){
                    thread.join().unwrap();
                }
            }
        }
    }

    impl<T: Send + 'static > ThreadPool<T>{
        pub fn new(size: usize) -> ThreadPool<T>{
            assert!(size > 0);
            let (sender, receiver) = mpsc::channel();
            let (sender_thread_result, receiver_thread_result) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            let mut workers = Vec::with_capacity(size);
            for id in 0..size {
                workers.push(Worker::new(
                    id,
                    Arc::clone(&receiver),
                    sender_thread_result.clone()
                ));
            }
            ThreadPool {
                workers,
                receiver_thread_result,
                sender
            }
        }

        pub fn execute(&self, operation: Operation<T>) {
            self.sender.send(Message::Operation(operation)).unwrap();
        }

        pub fn get_receiver(&self) -> &Receiver<Result<T>>{
            &self.receiver_thread_result
        }
    }
}

