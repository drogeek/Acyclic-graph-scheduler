//todo use Arg<T> intsead of Vec<T>
pub type Job<T> = fn(Vec<T>) -> T;

pub mod thread {
    use std::thread::JoinHandle;
    use petgraph::graph::NodeIndex;
    use std::sync::{Arc, Mutex, mpsc};
    use std::sync::mpsc::{Sender, Receiver};
    use std::thread;
    //use crate::{Job, ExecutableGraphType};
    use crate::Job;

    #[derive(Debug)]
    pub struct Result<T> {
        pub node_id: NodeIndex,
        pub value: T,
    }

    pub enum GraphMessage<T> {
        Starting(NodeIndex),
        Finished(Result<T>)
    }

    enum Message<T> {
        Operation(Operation<T>),
        Terminate
    }

    pub struct Operation<T>{
        pub node_id: NodeIndex,
        pub data: Vec<T>,
        pub f: Job<T>
    }
    impl<T> Operation<T>{
        pub fn new(node_id: NodeIndex, data: Vec<T>, f: Job<T>) -> Operation<T>{
            Operation{
                node_id,
                data,
                f
            }
        }
    }

    struct Worker {
        thread: Option<JoinHandle<()>>
    }

    impl Worker {
        fn new<T: Send + 'static>(_id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message<T>>>>, sender: Sender<GraphMessage<T>>)
                                  -> Worker {
            let thread = thread::spawn(move || loop {
                let receiver = Arc::clone(&receiver);
                let msg = receiver.lock().unwrap().recv().unwrap();
                match msg {
                    Message::Operation(Operation{node_id, data, f}) => {
                        sender.send(GraphMessage::Starting(node_id)).unwrap();
//                        println!("{} ({:?}) is being executed  by Worker {}", name, node_id, id);
                        sender.send(GraphMessage::Finished(Result {
                            node_id,
                            value: f(data)
                        })).unwrap();
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
        receiver_thread_result: Receiver<GraphMessage<T>>,
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

        pub fn get_receiver(&self) -> &Receiver<GraphMessage<T>>{
            &self.receiver_thread_result
        }
    }
}

