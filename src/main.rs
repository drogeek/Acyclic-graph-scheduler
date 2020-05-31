use petgraph::{Graph, Direction};
use petgraph::visit::EdgeRef;
use petgraph::graph::{NodeIndex, Node};
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use zama_challenge::Job;
use zama_challenge::thread;
use zama_challenge::thread::{ ThreadPool, Operation };
use petgraph::dot::Config::NodeIndexLabel;
use petgraph::csr::Neighbors;
use std::time::Duration;
use std::fmt::Display;
use std::borrow::BorrowMut;

type Args<T> = Vec<T>;

//returns the index of the first occurence in vec
fn find_index<T: Eq>(vec: &Vec<T>, value: &T) -> Option<usize>{
    for (idx, other) in vec.iter().enumerate(){
        if other == value{
            return Some(idx)
        }
    }
    None
}

struct ExecutableGraph<T: Send> {
    graph: Graph<Arc<Job<T>>, Option<T>>,
    thread_pool: ThreadPool<T>,
    initial_nodes: Vec<(NodeIndex, Args<T>)>,
    current_nodes: Vec<NodeIndex>,
    inprocess_nodes: Vec<NodeIndex>,
    finished_nodes: Vec<NodeIndex>,
    name_map: HashMap<NodeIndex, String>
}

#[derive(Debug)]
enum NodeAddingError{
    Circuit,
}


impl<T: Send + Copy + Display + 'static> ExecutableGraph<T>{
    pub fn new(thread_pool: ThreadPool<T>) -> ExecutableGraph<T>{
        ExecutableGraph {
            graph: Graph::<Arc<Job<T>>, Option<T>>::new(),
            current_nodes: Vec::new(),
            finished_nodes: Vec::new(),
            initial_nodes: Vec::new(),
            inprocess_nodes: Vec::new(),
            name_map: HashMap::new(),
            thread_pool,
        }
    }

    pub fn new_random(node_number: usize, thread_pool: ThreadPool<u32>) -> ExecutableGraph<u32>{
        let mut eg = ExecutableGraph::new(thread_pool);
        let init_sum = eg.add_initial_node(
            |x| x.iter().sum(),
            vec![5,2,3],
            Some("init_sum")
        );
        let init_sum2 = eg.add_initial_node(
            |x| x.iter().sum(),
            vec![3,3,3,4,4],
            Some("init_sum2")
        );
        let init_prod = eg.add_initial_node(
            |x| x.iter().product(),
            vec![2,3,5,4],
            Some("init_prod")
        );
        let sleep = eg.add_node(
            |x| {
                let n = x.len();
                std::thread::sleep(Duration::new((n*4) as u64, 0));
                n as u32
            },
            vec![init_sum, init_prod],
            Some("sleep")
        ).unwrap();
        let sqsum = eg.add_node(
            |x| x.iter().map(|x| x.pow(2)).sum(),
            vec![init_sum, sleep],
            Some("sqsum")
        ).unwrap();

        let sum2 = eg.add_node(
            |x| x.iter().sum(),
            vec![init_prod, init_sum2],
            Some("sum2")
        ).unwrap();

        let sum3 = eg.add_node(
            |x| x.iter().sum(),
            vec![sum2],
            Some("sum3")
        ).unwrap();

        let sum4 = eg.add_node(
            |x| x.iter().sum(),
            vec![sum2, sqsum],
            Some("sum4")
        ).unwrap();
        eg
    }

    fn is_done(&self) -> bool{
        self.inprocess_nodes.is_empty() && self.current_nodes.is_empty()
    }

    pub fn add_node(
        &mut self,
        f: impl Fn(Vec<T>) -> T + Send + Sync + 'static, parents: Vec<NodeIndex>,
        name: Option<&str>
    ) -> Result<NodeIndex, NodeAddingError> {
        assert!(!parents.is_empty());
        //todo check that the new node doesn't create a circuit

        let new_node = self.graph.add_node(Arc::new(f));
        if let Some(name) = name {
            self.name_map.insert(new_node, String::from(name));
        }

        for node in parents{
            self.graph.add_edge(node, new_node, None);
        }
        Ok(new_node)
    }

    pub fn add_initial_node(
        &mut self,
        f: impl Fn(Vec<T>) -> T + Send + Sync + 'static, arguments: Args<T>,
        name: Option<&str>
    ) -> NodeIndex {
        let node = self.graph.add_node(Arc::new(f));
        if let Some(name) = name {
            self.name_map.insert(node, String::from(name));
        }
        self.initial_nodes.push((node, arguments));
        node
    }

    fn execute_valid_nodes(&mut self){
        // look for current nodes that have parents who finished their task, and haven't been already
        // executed or are being executed. Execute them, remove them from the current nodes, and add
        // all their children to it
        let nodes_to_execute: Vec<_> = self.current_nodes.iter().copied()
            .filter(|node| {
                !self.finished_nodes.contains(&node) && !self.inprocess_nodes.contains(&node) &&
                self.graph.neighbors_directed(*node, Direction::Incoming)
                    .all(|neighbour| self.finished_nodes.contains(&neighbour))
            }).collect();
        //execute nodes and add their children into current nodes (without duplicate)
        for node in nodes_to_execute{
            self.current_nodes.append(&mut self.graph.neighbors_directed(node, Direction::Outgoing)
                .filter(|node| !self.current_nodes.contains(node)).collect::<Vec<_>>());
            self.execute_node(node);
        }

    }

    pub fn start(&mut self) {
        //launch the initial nodes in threads,
        //then wait for messages to know which ones returned
        //and determine which ones to execute next
        self.execute_initial_nodes();

        while !self.is_done() {
            //todo handle the possible error?
            if let Ok(thread::Result { node_id: node, value }) = self.thread_pool.get_receiver().recv() {
                self.print_result(node, value);
                self.finished_nodes.push(node);
                self.inprocess_nodes.remove(find_index(&self.inprocess_nodes, &node).unwrap());

                // since apparently there isn't a way to edit the weight of edges,
                // simply duplicate the edge to hold the result
                // do it in 2 loops because `edges_directed` borrows immutably, so
                // `add_edge` can't be called on it in the same scope
                let edges_info: Vec<_> = self.graph.edges_directed(node, Direction::Outgoing)
                    .map(|e| (e.id(), e.target(), e.source()) )
                    .collect();
                for (id, target, source) in edges_info{
                    self.graph.add_edge(source, target, Some(value));
                }
                self.execute_valid_nodes();
            }
        }
    }

    fn execute_initial_nodes(&mut self){
        let mut initial_nodes = self.initial_nodes.iter()
            .map(|(node, _)| *node)
            .collect();
        self.current_nodes.append(&mut initial_nodes);

        //copy the list so we can iterate over it while modifying `self` afterward
        let mut initial_nodes_copy = Vec::new();
        for (node, arguments) in self.initial_nodes.iter() {
            initial_nodes_copy.push((node.clone(), arguments.clone()));
        }
        //execute initial nodes and add their children (without duplicate)
        for (node, arguments) in initial_nodes_copy {
            self.execute_node_with_arg(node, arguments);
            self.current_nodes.append(&mut self.graph.neighbors_directed(node, Direction::Outgoing)
                .filter(|node| !self.current_nodes.contains(node)).collect::<Vec<_>>());
        }
    }

    fn print_result(&self, node: NodeIndex, value: T){
        if let Some(name) = self.name_map.get(&node){
            println!("{:?} aka {:?} returned with value {}", name, node, value);
        }
        else {
            println!("{:?} returned with value {}", node, value);
        }
    }


    fn execute_node(&mut self, node: NodeIndex){
        // build arguments from the incoming edges of the node, ignoring the edges that
        // were there to define the connection (these edges are duplicated with the
        // result of operations in `start`)
        let arguments : Vec<T> = self.graph.edges_directed(node, Direction::Incoming)
            .filter_map(|x| *x.weight())
            .collect();
        self.execute_node_with_arg(node, arguments);
    }

    fn execute_node_with_arg(&mut self, node: NodeIndex, arguments: Vec<T>){
        self.inprocess_nodes.push(node);
        self.current_nodes.remove(find_index(&self.current_nodes, &node).unwrap());
        self.thread_pool.execute(Operation::new(node, arguments, &self.graph));
    }
}

fn main(){
    let thread_pool = ThreadPool::<u32>::new(4);
    let mut g = ExecutableGraph::<u32>::new_random(3, thread_pool);
    g.start();

}