use petgraph::{Graph, Direction};
use petgraph::visit::EdgeRef;
use petgraph::graph::{NodeIndex};
use std::collections::HashMap;
use zama_challenge::Job;
use zama_challenge::thread;
use zama_challenge::thread::{ThreadPool, GraphMessage};
use std::fmt::Display;
use petgraph::dot::Dot;
use std::process::Command;
use std::fs::File;
use std::io::Write;
use rand::{thread_rng, Rng};
use rand::distributions::Uniform;
use std::cmp::min;
use rand::distributions::Distribution;
use std::rc::Rc;
use std::cell::RefCell;

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


#[derive(Clone)]
pub struct Operation<T: Send> {
    pub f: fn(Args<T>) -> T,
    pub name: String
}


//Each node represent an operation to execute.
//There are two ways to execute one:
//- decrementing the counter by calling the `decrement` method, once it reaches zero, it gets
//executed with the arguments being built from the incoming edges
//- executing it directly with
//specified arguments by calling the `execute` method
#[derive(Clone)]
struct Node<T: Send> {
    f: Job<T>,
    counter: Rc<RefCell<Option<u32>>>,
    pub name: String,
    index: Option<NodeIndex>,
    graph: Rc<RefCell<Graph<Node<T>, Option<T>>>>,
    thread_pool: Rc<RefCell<ThreadPool<T>>>,
}

impl<T: Clone + Send + 'static> Node<T> {
    pub fn new(f: Job<T>,
               parents: Option<Vec<NodeIndex>>,
               name: String,
               graph: Rc<RefCell<Graph<Node<T>, Option<T>>>>,
               thread_pool: Rc<RefCell<ThreadPool<T>>>,
    ) -> NodeIndex {

        match parents {
            Some(parents) => {
                let node = Node {
                    f,
                    counter: Rc::new(RefCell::new(Some(parents.len() as u32))),
                    name,
                    graph: Rc::clone(&graph),
                    index: None,
                    thread_pool
                };

                let mut graph = graph.borrow_mut();
                let node_idx = graph.add_node(node);
                graph[node_idx].index = Some(node_idx);
                for parent in parents{
                    graph.add_edge(parent, node_idx, None);
                }
                node_idx
            }
            None => {
                let node = Node {
                    f,
                    counter: Rc::new(RefCell::new(None)),
                    name,
                    graph: Rc::clone(&graph),
                    index: None,
                    thread_pool
                };

                let mut graph = graph.borrow_mut();
                let node_idx = graph.add_node(node);
                graph[node_idx].index = Some(node_idx);
                node_idx
            }
        }

    }

    pub fn index(&self) -> NodeIndex{
        self.index.unwrap()
    }

    pub fn execute(&mut self, arguments: Vec<T>){

        self.thread_pool.borrow().execute(thread::Operation::new(
            self.index(),
            arguments,
            self.f)
        );
    }

    pub fn decrement(&mut self){
        let counter = self.counter.borrow().clone();
        if let Some(mut counter) = counter {
            counter -= 1;
            if counter == 0 {
                let arguments: Vec<_> = self.graph.borrow().edges_directed(self.index(), Direction::Incoming)
                    .map(|x| x.weight().as_ref().unwrap().clone())
                    .collect();
                self.execute(arguments);
            }
            *self.counter.borrow_mut() = Some(counter);
        }
    }
}

//This is the structure that holds the graph scheduler, it needs a thread pool instance
pub struct ExecutableGraph<T: Send> {
    graph: Rc<RefCell<Graph<Node<T>, Option<T>>>>,
    thread_pool: Rc<RefCell<ThreadPool<T>>>,
    initial_nodes: Vec<(NodeIndex, Args<T>)>,
    parallel_nodes: Vec<NodeIndex>,
    execution_order: Vec<NodeIndex>,
    counter: u32,
}

impl<T: Send + Copy + Display + ToString + 'static> ExecutableGraph<T>{
    pub fn new(thread_pool: ThreadPool<T>) -> ExecutableGraph<T>{
        ExecutableGraph {
            graph: Rc::new(RefCell::new(Graph::<Node<T>, Option<T>>::new())),
            initial_nodes: Vec::new(),
            execution_order: Vec::new(),
            parallel_nodes: Vec::new(),
            thread_pool: Rc::new(RefCell::new(thread_pool)),
            counter: 0,
        }
    }

    fn pick_random_operation(cumulated_distribution: &Vec<f32>,
                                 random_value: f32,
                                 values: &Vec<Operation<T>>,
                                 name_counter: &mut HashMap<String, u32>)
                                 -> Result<Operation<T>, String>{
        for (idx, threshold) in cumulated_distribution.iter().enumerate(){
            if random_value <= *threshold {
                let mut operation = values.get(idx).unwrap().clone();
                let count = name_counter.entry(operation.name.clone()).or_insert(0);
                *count += 1;
                operation.name = format!("{}{}",operation.name, count);
                return Ok(operation)
            }
        }
        Err(String::from("Error, either the cumulated_distibution is wrong, or random_value is > 1"))
    }

    // return at most `number` distinct parents picked randomly
    fn pick_random_parents(nodes: &Vec<NodeIndex>, number: usize) -> Vec<NodeIndex>{
        let between = Uniform::from(0..nodes.len());
        let mut rng = thread_rng();
        let mut parents : Vec<NodeIndex> = Vec::new();
        let number = min(number, nodes.len());

        // we randomly pick distinct parents until we got the number requested, or until there is no more
        // distinct parents
        let mut attempted_nodes = Vec::new();
        while parents.len() < number && attempted_nodes.len() != nodes.len() {
            let candidate_node = *nodes.get(between.sample(&mut rng)).unwrap();
            attempted_nodes.push(candidate_node);
            if !parents.contains(&candidate_node){
                parents.push(candidate_node);
            }
        }
        return parents
    }

    // generate a random graph, where each node are randomly pooled from `operations` according to
    // the probability distribution `distribution`, which must sum to 1.
    // If no distribution is given, all operations are considered equiprobable.
    // `max_parents` determines how many possible parents each node can have.
    // The number of `arguments` determines how many initial nodes (and therefore by design, the number of possible connected graphs) there are,
    // and each argument feeds one of them
    pub fn generate_random(node_number: usize,
                           max_parents: usize,
                           arguments: Vec<Args<T>>,
                           operations: Vec<Operation<T>>,
                           distribution: Option<Vec<f32>>,
                           thread_pool: ThreadPool<T>)
                           -> ExecutableGraph<T>{

        assert!(arguments.len() > 0);
        assert!(node_number >= arguments.len());


        let eg = ExecutableGraph::new(thread_pool);
        if let Some(distribution) = distribution {
            assert_eq!(distribution.len(), operations.len());
            assert_eq!(distribution.iter().sum::<f32>(), 1.0);
            return Self::construct_random_graph(eg, node_number, max_parents, distribution, arguments, operations)
        }
        else {
            let distribution = vec![1. / (operations.len() as f32); operations.len()];
            return Self::construct_random_graph(eg,node_number, max_parents, distribution, arguments, operations)
        }
    }

    // see the `generate_random` function, this is just a subtask of it
    fn construct_random_graph(mut eg: ExecutableGraph<T>,
                              node_number: usize,
                              max_parents: usize,
                              distribution: Vec<f32>,
                              arguments: Vec<Vec<T>>,
                              operations: Vec<Operation<T>>)
                              -> ExecutableGraph<T> {
        let mut rng = thread_rng();
        let mut nodes = Vec::new();
        let cumulated_distribution: Vec<f32> = distribution.iter()
            .scan(0.0, |acc, &x| {
                *acc = *acc + x;
                Some(*acc)
            })
            .collect();
        let mut name_counter = HashMap::new();
        let initial_node_number  = arguments.len();
        for arg in arguments {
            let r = rng.gen();
            let operation = Self::pick_random_operation(&cumulated_distribution,
                                                        r,
                                                        &operations,
                                                        &mut name_counter).unwrap();
            nodes.push(eg.add_initial_node(operation, arg));
        }

        for _ in 0..node_number - initial_node_number {
            let parents_nbr = rng.gen_range(1, max_parents+1);
            let r = rng.gen();

            let operation = Self::pick_random_operation(&cumulated_distribution,
                                                        r,
                                                        &operations,
                                                        &mut name_counter).unwrap();
            let parents = Self::pick_random_parents(&nodes, parents_nbr);
            nodes.push(eg.add_node(operation, parents));
        }
        eg
    }

    // return a string containing the dot format of the graph
    pub fn get_dot(&self) -> String{
        // we reconstruct a graph with the same node and edges, except that node and edges contains
        // strings corresponding respectively to the node's name and the edge's value

        let graph = self.graph.borrow();
        let mut new_graph: Graph<String, String> = Graph::with_capacity(graph.node_count(), graph.edge_count());
        let nodes : Vec<_> = graph.node_indices().into_iter().collect();

        // we make sure the node index correspond in both graphs
        let mut old_to_new_nodes = HashMap::new();
        for node in nodes.iter() {
            let new_node = new_graph.add_node(graph[*node].name.clone());
            old_to_new_nodes.insert(node, new_node);
        }

        // recreate the edges
        for node in nodes.iter() {
            let edges = graph.edges(*node);
            for edge in edges {
                let source = old_to_new_nodes.get(&edge.source()).unwrap();
                let target = old_to_new_nodes.get(&edge.target()).unwrap();

                // if the edge has no value, print an empty string
                let weight = match edge.weight() {
                    Some(value) => value.to_string(),
                    _ => String::new()
                };
                new_graph.add_edge(*source, *target, weight);
            }
        }
        format!("{}", Dot::new(&new_graph))
    }

    // save the dot format in a file called `executable_graph.dot` in the working directory
    pub fn save_dot(&self){
        let mut file = File::create("executable_graph.dot").expect("Can't create `executable_graph.dot`");
        file.write_all(self.get_dot().as_bytes()).expect("Couldn't write into `executable_graph.dot`");
    }

    // generate the dot graph, save it, generate a png image of the graph, and open it
    // note: you need graphviz installed
    pub fn show_graph(&self){
        if cfg!(windows){
            let script_name = "generate_and_show_dot.bat";
            self.save_dot();
            Command::new(format!(".\\{}", script_name))
                .status()
                .expect(&format!("{} failed", script_name));
        }
        else {
            let script_name = "generate_and_show_dot.sh";
            self.save_dot();
            Command::new(format!("./{}", script_name))
                .status()
                .expect(&format!("{} failed", script_name));
        }

    }


    // we consider the process done when there are no nodes left and no node being processed
    fn is_done(&self) -> bool{
        self.counter == 0
    }

    // in the end, we shouldn't need it if we use the `add_node` and `add_initial_node` functions
    /*
    fn has_cycle(graph: &Graph<Arc<Job<T>>, Option<T>>, starting_node: NodeIndex) -> bool{
        let mut dfs = Dfs::new(graph, starting_node);
        //skip the first value, which is `starting_node` itself
        dfs.next(&graph);
        while let Some(node) = dfs.next(&graph){
            if node == starting_node{
                return true
            }
        }
        false
    }
    */

    // add the node with the operation `f`, name `name`, and whose parents are `parents`.
    // there must be at least 1 parent, and they must exist.
    // there are no constraint about duplicate parents
    pub fn add_node(
        &mut self,
        operation: Operation<T>,
        parents: Vec<NodeIndex>,
    ) -> NodeIndex {
        assert_ne!(parents.len(), 0);
        let Operation{f, name} = operation;
        self.counter += 1;
        Node::new(f, Some(parents), name, Rc::clone(&self.graph), Rc::clone(&self.thread_pool))
    }

    // add an orphean node which will be one of the entry points of the graph, and which will
    // receive `arguments` as input
    pub fn add_initial_node(
        &mut self,
        operation: Operation<T>,
        arguments: Args<T>,
    ) -> NodeIndex {
        let Operation{f, name} = operation;
        let node = Node::new(f, None, name, Rc::clone(&self.graph), Rc::clone(&self.thread_pool));
        self.counter += 1;
        self.initial_nodes.push((node, arguments));
        node
    }


    pub fn start(&mut self) {
        //launch the initial nodes in threads,
        //then wait for messages to know which ones returned
        //and determine which ones to execute next
        self.execute_initial_nodes();

        while !self.is_done() {
            //todo handle the possible error?
            if let Ok(message_type)  = self.thread_pool.borrow().get_receiver().recv() {
                match message_type {
                   GraphMessage::Finished(thread::Result { node_id, value }) => {

                       self.counter -= 1;
                       self.parallel_nodes.remove(find_index(&self.parallel_nodes, &node_id).unwrap());
                       self.print_result(node_id, Some(value));

                       // update the weight of outgoing edges with the result of the operation
                       let edges_info: Vec<_> = self.graph.borrow().edges_directed(node_id, Direction::Outgoing)
                           .map(|e| e.target())
                           .collect();
                       for target in edges_info{
                           self.graph.borrow_mut().update_edge(node_id, target, Some(value));
                           self.graph.borrow()[target].clone().decrement();
                       }
                   },
                    GraphMessage::Starting(node) => {
                        self.parallel_nodes.push(node);
                        self.execution_order.push(node);
                        self.print_result(node, None);
                    }
                }

            }
        }

        println!();
        println!("The execution order was:");
        for (idx, (node, name)) in self.execution_order.iter()
            .map(|node| (node, self.graph.borrow()[*node].name.clone())).enumerate(){
            println!("{}: {} ({:?})", idx+1, name, node);
        }
    }

    fn execute_initial_nodes(&mut self){
        for (initial_node, arguments) in self.initial_nodes.iter(){
            self.graph.borrow_mut()[*initial_node].execute(arguments.clone());
        }
    }

    fn print_result(&self, node: NodeIndex, value: Option<T>){
        let name = self.graph.borrow()[node].name.clone();
        match value {
            Some(value) => println!("\t{} returned with value {}\n{:?}",
                                    name,
                                    value,
                                    self.parallel_nodes.iter()
                                        .map(|node| self.graph.borrow()[*node].name.clone())
                                        .collect::<Vec<_>>()
            ),
            None => println!("{:?}", self.parallel_nodes.iter()
                .map(|node| self.graph.borrow()[*node].name.clone())
                .collect::<Vec<_>>())
        }
    }
}
