use zama_challenge::thread::ThreadPool;
use crate::executable_graph::{Operation, ExecutableGraph};
use std::time::Duration;
use std::num::Wrapping;

mod executable_graph;


fn main(){
    let thread_pool = ThreadPool::<Wrapping<i32>>::new(6);
//    let mut g = predefined_graph(thread_pool);
   /*
    let mut g = ExecutableGraph::generate_random(
        100,
        4,
        vec![vec![3,4], vec![4,5,1,1], vec![3,7,12,20,55], vec![1,2,3]],
        vec![
            Operation{f:|x| x.iter().sum(), name: String::from("sum")},
            Operation{f:|x| x[0]+1, name: String::from("incr")},
            Operation{f:|x| x.iter().fold(0, |acc, x| acc - *x), name: String::from("sub")},
            Operation{
                f:|x| {
                    let n = x.len();
                    std::thread::sleep(Duration::new(n as u64, 0));
                    n as i32
                },
                name: String::from("sleep")
            },
        ],
//        None,
        Some(vec![0.2,0.4,0.3,0.1]),
        thread_pool
    );*/
    let mut g = ExecutableGraph::<Wrapping<i32>>::generate_random(
        10000,
        500,
        vec![[3,4].iter().map(|x| Wrapping(*x)).collect::<Vec<_>>(),
        [3,4,5].iter().map(|x| Wrapping(*x)).collect::<Vec<_>>(),
        [2,3,4].iter().map(|x| Wrapping(*x)).collect::<Vec<_>>(),
        [4].iter().map(|x| Wrapping(*x)).collect::<Vec<_>>()
        ],
        vec![
            Operation{f:|x| x.iter().sum(), name: String::from("sum")},
            Operation{f:|x| x.iter().fold(Wrapping(0), |acc, x| acc - *x), name: String::from("sub")},
            Operation{
                f:|x| {
                    let n = x.len();
                    std::thread::sleep(Duration::new(1, 0));
                    Wrapping(n as i32)
                },
                name: String::from("sleep")
            },
        ],
        Some(vec![0.5,0.497,0.003]),
        thread_pool
    );


    g.start();

}

pub fn predefined_graph(thread_pool: ThreadPool<u32>) -> ExecutableGraph<u32>{
    let mut eg = ExecutableGraph::new(thread_pool);
    let init_sum = eg.add_initial_node(
        Operation{f:|x| x.iter().sum(),
            name: String::from("init_sum")},
        vec![5,2,3],
    );
    let init_sum2 = eg.add_initial_node(
        Operation{f:|x| x.iter().sum(),
            name:String::from("init_sum2")},
        vec![3,3,3,4,4],
    );
    let init_prod = eg.add_initial_node(
        Operation{f: |x| x.iter().product(),
            name: String::from("init_prod")},
        vec![2,3,5,4],
    );
    let sleep = eg.add_node(
        Operation{ f:|x| {
            let n = x.len();
            std::thread::sleep(Duration::new((n*4) as u64, 0));
            n as u32
        },
            name: String::from("sleep")
        },
        vec![init_sum, init_prod],
    );
    let sqsum = eg.add_node(
        Operation{f: |x| x.iter().map(|x| x.pow(2)).sum(),
            name: String::from("sqsum")},
        vec![init_sum, sleep, init_prod],
    );

    let sum2 = eg.add_node(
        Operation{f: |x| x.iter().sum(),
            name: String::from("sum2")},
        vec![init_prod, init_sum2],
    );

    let _sum3 = eg.add_node(
        Operation{f:|x| x.iter().sum(),
            name: String::from("sum3")},
        vec![sum2],
    );

    let _sum4 = eg.add_node(
        Operation{f: |x| x.iter().sum(),
            name:String::from("sum4")},
        vec![sum2, sqsum],
    );
    eg
}
