use zama_challenge::thread::ThreadPool;
use crate::executable_graph::{Operation, ExecutableGraph};
use std::time::Duration;

mod executable_graph;


fn main(){
    let thread_pool = ThreadPool::<u32>::new(4);
//    let mut g = new_predefined(thread_pool);
    let mut g = ExecutableGraph::generate_random(
        100,
        3,
        vec![vec![3,4], vec![4,5,1,1], vec![3,7,12,20,55]],
        vec![
            Operation{f:|x| x.iter().sum(), name: String::from("sum")},
            Operation{f:|x| x[0]+1, name: String::from("incr")},
            Operation{
                f:|x| {
                    let n = x.len();
                    std::thread::sleep(Duration::new(n as u64, 0));
                    n as u32
                },
                name: String::from("sleep")
            },
        ],
        Some(vec![0.1,0.6,0.3]),
        thread_pool
    );

    g.show_graph();
    g.start();
    g.show_graph();

}

pub fn new_predefined(thread_pool: ThreadPool<u32>) -> ExecutableGraph<u32>{
    let mut eg = ExecutableGraph::new(thread_pool);
    let init_sum = eg.add_initial_node(
        |x| x.iter().sum(),
        vec![5,2,3],
        "init_sum"
    );
    let init_sum2 = eg.add_initial_node(
        |x| x.iter().sum(),
        vec![3,3,3,4,4],
        "init_sum2"
    );
    let init_prod = eg.add_initial_node(
        |x| x.iter().product(),
        vec![2,3,5,4],
        "init_prod"
    );
    let sleep = eg.add_node(
        |x| {
            let n = x.len();
            std::thread::sleep(Duration::new((n*4) as u64, 0));
            n as u32
        },
        vec![init_sum, init_prod],
        "sleep"
    );
    let sqsum = eg.add_node(
        |x| x.iter().map(|x| x.pow(2)).sum(),
        vec![init_sum, sleep, init_prod],
        "sqsum"
    );

    let sum2 = eg.add_node(
        |x| x.iter().sum(),
        vec![init_prod, init_sum2],
        "sum2"
    );

    let _sum3 = eg.add_node(
        |x| x.iter().sum(),
        vec![sum2],
        "sum3"
    );

    let _sum4 = eg.add_node(
        |x| x.iter().sum(),
        vec![sum2, sqsum],
        "sum4"
    );
    eg
}