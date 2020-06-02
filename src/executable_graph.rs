use petgraph::{Graph, Direction};
use petgraph::visit::{EdgeRef, Dfs};
use petgraph::graph::{NodeIndex};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use zama_challenge::Job;
use zama_challenge::thread;
use zama_challenge::thread::ThreadPool;
use std::time::Duration;
use std::fmt::Display;
use petgraph::dot::Dot;
use std::process::Command;
use std::fs::File;
use std::io::Write;
use rand::{thread_rng, Rng};
use rand::distributions::Uniform;
use std::cmp::min;
use rand::distributions::Distribution;
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

pub struct ExecutableGraph<T: Send> {
    graph: Graph<Job<T>, Option<T>>,
    thread_pool: ThreadPool<T>,
    initial_nodes: Vec<(NodeIndex, Args<T>)>,
    current_nodes: HashSet<NodeIndex>,
    inprocess_nodes: Vec<NodeIndex>,
    finished_nodes: HashSet<NodeIndex>,
    execution_order: Vec<NodeIndex>,
    name_map: HashMap<NodeIndex, String>
}


impl<T: Send + Copy + Display + ToString + 'static> ExecutableGraph<T>{
    pub fn new(thread_pool: ThreadPool<T>) -> ExecutableGraph<T>{
        ExecutableGraph {
            graph: Graph::<Job<T>, Option<T>>::new(),
            current_nodes: HashSet::new(),
            finished_nodes: HashSet::new(),
            initial_nodes: Vec::new(),
            inprocess_nodes: Vec::new(),
            name_map: HashMap::new(),
            execution_order: Vec::new(),
            thread_pool,
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
            let distribution = distribution;
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
            let Operation{f, name} = Self::pick_random_operation(&cumulated_distribution, r, &operations, &mut name_counter).unwrap();
            nodes.push(eg.add_initial_node(f, arg, &name));
        }

        for _ in 0..node_number - initial_node_number {
            let parents_nbr = rng.gen_range(1, max_parents+1);
            let r = rng.gen();

            let Operation{f, name} = Self::pick_random_operation(&cumulated_distribution, r, &operations, &mut name_counter).unwrap();
            let parents = Self::pick_random_parents(&nodes, parents_nbr);
            nodes.push(eg.add_node(f, parents, &name));
        }
        eg
    }

    // return a string containing the dot format of the graph
    pub fn get_dot(&self) -> String{
        // we reconstruct a graph with the same node and edges, except that node and edges contains
        // strings corresponding respectively to the node's name and the edge's value

        let mut new_graph: Graph<String, String> = Graph::with_capacity(self.graph.node_count(), self.graph.edge_count());
        let nodes : Vec<_> = self.graph.node_indices().into_iter().collect();

        // we make sure the node index correspond in both graphs
        let mut old_to_new_nodes = HashMap::new();
        for node in nodes.iter() {
            let new_node = new_graph.add_node(String::from(self.name_map.get(&node).unwrap()));
            old_to_new_nodes.insert(node, new_node);
        }

        // recreate the edges
        for node in nodes.iter() {
            let edges = self.graph.edges(*node);
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
        let script_name = "generate_and_show_dot.sh";
        self.save_dot();
        Command::new(format!("./{}", script_name))
            .status()
            .expect(&format!("{} failed", script_name));
    }




    // we consider the process done when there are no nodes left and no node being processed
    fn is_done(&self) -> bool{
        self.inprocess_nodes.is_empty() && self.current_nodes.is_empty()
    }

    // in the end, we shouldn't need it if we use the `add_node` and `add_initial_node` functions
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

    // add the node with the operation `f`, name `name`, and whose parents are `parents`.
    // there must be at least 1 parent, which must already exist.
    // we put no constraint on duplicate parents
    pub fn add_node(
        &mut self,
        f: fn(Vec<T>) -> T,
        parents: Vec<NodeIndex>,
        name: &str
    ) -> NodeIndex {
        assert_ne!(parents.len(), 0);
        let new_node = self.graph.add_node(f);
        for node in parents{
            self.graph.add_edge(node, new_node, None);
        }

        self.name_map.insert(new_node, String::from(name));
        new_node
    }

    // add an orphean node which will be one of the entry points of the graph, and which will
    // receive `arguments` as input
    pub fn add_initial_node(
        &mut self,
        f: fn(Vec<T>) -> T,
        arguments: Args<T>,
        name: &str
    ) -> NodeIndex {
        let node = self.graph.add_node(f);
        self.name_map.insert(node, String::from(name));
        self.initial_nodes.push((node, arguments));
        node
    }

    fn execute_valid_nodes(&mut self){
        // look for current nodes that have parents who finished their task, and haven't been already
        // executed or are being executed. Execute them, removing them from the current nodes, and add
        // all their children to it
        let nodes_to_execute: Vec<_> = self.current_nodes.iter().copied()
            .filter(|node| {
                !self.finished_nodes.contains(&node) && !self.inprocess_nodes.contains(&node) &&
                    self.graph.neighbors_directed(*node, Direction::Incoming)
                        .all(|neighbour| self.finished_nodes.contains(&neighbour))
            }).collect();
        //execute nodes and add their children into current nodes (without duplicate)
        for node in nodes_to_execute{
            for child in self.graph.neighbors_directed(node, Direction::Outgoing) {
                self.current_nodes.insert(child);
            };
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
                self.finished_nodes.insert(node);
                self.inprocess_nodes.remove(find_index(&self.inprocess_nodes, &node).unwrap());

                // update the weight of outgoing edges with the result of the operation
                let edges_info: Vec<_> = self.graph.edges_directed(node, Direction::Outgoing)
                    .map(|e| e.target())
                    .collect();
                for target in edges_info{
                    self.graph.update_edge(node, target, Some(value));
                }
                self.execute_valid_nodes();
            }
        }

        println!();
        println!("The execution order was:");
        for (idx, (node, name)) in self.execution_order.iter()
            .map(|node| (node, self.name_map.get(node).unwrap())).enumerate(){
            println!("{}: {} ({:?})", idx+1, name, node);
        }
    }

    fn execute_initial_nodes(&mut self){
        for (initial_node, _) in self.initial_nodes.iter(){
            self.current_nodes.insert(*initial_node);
        }

        //copy the list so we can iterate over it while modifying `self` afterward
        let mut initial_nodes_copy = Vec::new();
        for (node, arguments) in self.initial_nodes.iter() {
            initial_nodes_copy.push((node.clone(), arguments.clone()));
        }
        //execute initial nodes and add their children (without duplicate)
        for (node, arguments) in initial_nodes_copy {
            self.execute_node_with_arg(node, arguments);
            for child in self.graph.neighbors_directed(node, Direction::Outgoing) {
                self.current_nodes.insert(child);
            }
        }
    }

    fn print_result(&self, node: NodeIndex, value: T){
        let name = self.name_map.get(&node).unwrap();
        println!("{} ({:?}) returned with value {}", name, node, value);
    }


    fn execute_node(&mut self, node: NodeIndex){
        // build arguments from the incoming edges of the node, then execute it
        let arguments : Vec<T> = self.graph.edges_directed(node, Direction::Incoming)
            .filter_map(|x| *x.weight())
            .collect();
        self.execute_node_with_arg(node, arguments);
    }

    fn execute_node_with_arg(&mut self, node: NodeIndex, arguments: Vec<T>){
        if !self.execution_order.contains(&node){
            self.execution_order.push(node);
        }
        self.inprocess_nodes.push(node);
        self.current_nodes.remove(&node);
        self.thread_pool.execute(thread::Operation::new(
            node,
            arguments,
            String::from(self.name_map.get(&node).unwrap()),
            &self.graph)
        );
    }
}