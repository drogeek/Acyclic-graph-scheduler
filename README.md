# Description
This is a scheduler to compute an acyclic graph in parallel (with a pool of threads), where nodes represent operations, and edges data to process

# How to run the project : 
On Linux : 
```console 
sudo apt install graphviz 
cargo run
```
I couldn’t test on Windows, but graphviz has been ported for Windows, so it
should work if installed.  

# Description

**main.rs** contains an example of both way to define an *ExecutableGraph* 

**lib.rs** contains the implementation of the thread pool

**executable_graph.rs** contains the implementation of the *ExecutableGraph* 


First a *ThreadPool* instance is necessary. We get one with the *ThreadPool::new*
method, specifying how many threads we want.  
Then there are two ways to generate an *ExecutableGraph*, as shown in main.rs 
* using *ExecutableGraph::new* method, passing it an instance of *ThreadPool* 
    * then calling *add_initial_node* and *add_node* methods to iteratively construct the graph 
* using *ExecutableGraph::generate_random*, passing it 
    * the number of nodes it will generate 
    * the max number of parents a node can have 
    * a list of arguments, which defines how many initial nodes are created and what to feed them with 
    * a list of *Operation*, containing a closure and a name 
    * an optional probability distribution for the operations 
    * an instance of *ThreadPool* 

Then using the *start* method to run the graph, and *show_graph* to print a representation of it.  
If too many nodes are used, it’s probably wise not to use the *show_graph* method.

# Example
![example](https://github.com/drogeek/compute_acyclic_graph/blob/master/example.png)
