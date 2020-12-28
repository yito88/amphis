extern crate amphis;

use env_logger;
use std::sync::mpsc;
use std::sync::Arc;
use threadpool::ThreadPool;

/* For testing */
fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let num_elements: usize = args[0].parse().unwrap();
    let num_threads: usize = args[1].parse().unwrap();

    //seq_insert(num_elements);

    concurrent_insert(num_elements, num_threads);
}

fn seq_insert(num_elements: usize) {
    let config = amphis::config::Config::new();
    let kvs = amphis::kvs::KVS::new(config).unwrap();

    for i in 0..num_elements {
        let key = "k".to_string() + &i.to_string();
        let value = "v".to_string() + &i.to_string();
        kvs.put(&key.as_bytes().to_vec(), &value.as_bytes().to_vec())
            .unwrap();
    }

    for i in 0..num_elements {
        let key = format!("{}{}", "k", (&*i.to_string()));
        let expected = format!("{}{}", "v", (&*i.to_string()));
        match kvs.get(&key.as_bytes().to_vec()).unwrap() {
            Some(value) => {
                let actual = String::from_utf8(value.to_vec()).unwrap();

                match expected {
                    _ if expected == actual => println!("Get result {}", actual),
                    _ => panic!("expected: {}, actual: {}", expected, actual),
                }
            }
            None => panic!("expected: {}, actual: None", expected),
        };
    }
}

fn concurrent_insert(num_elements: usize, num_threads: usize) {
    let (tx, rx) = mpsc::channel();
    let pool = ThreadPool::new(if num_threads <= 1 { 1 } else { num_threads });

    let config = amphis::config::Config::new();
    let kvs = Arc::new(amphis::kvs::KVS::new(config).unwrap());

    for i in 0..num_threads {
        let each = kvs.clone();

        let tx = tx.clone();
        pool.execute(move || {
            for v in 0..num_elements {
                let key = format!("k{}:{}", v, i).as_bytes().to_vec();
                let value = format!("v{}:{}", v, i).as_bytes().to_vec();
                each.put(&key, &value).unwrap();
            }
            let result = 0;
            tx.send(result)
                .expect("channel will be there waiting for the pool");
        });
    }

    assert_eq!(rx.iter().take(num_threads).all(|r| r == 0), true);

    for i in 0..num_threads {
        let each = kvs.clone();

        let tx = tx.clone();
        pool.execute(move || {
            for v in 0..num_elements {
                let key = format!("k{}:{}", v, i);
                let expected = format!("v{}:{}", v, i);
                match each.get(&key.as_bytes().to_vec()).unwrap() {
                    Some(value) => {
                        let actual = String::from_utf8(value.to_vec()).unwrap();

                        match expected {
                            _ if expected == actual => println!("Get result {}", actual),
                            _ => panic!("expected: {}, actual: {}", expected, actual),
                        }
                    }
                    None => panic!("expected: {}, actual: None", expected),
                };
            }
            let result = 0;
            tx.send(result)
                .expect("channel will be there waiting for the pool");
        });
    }

    assert_eq!(rx.iter().take(num_threads).all(|r| r == 0), true);
}
