extern crate amphis;

//use std::sync::mpsc;
//use std::sync::Arc;
//use threadpool::ThreadPool;
use env_logger;

fn main() {
    env_logger::init();

    /*
    let n_jobs = 1;
    let (tx, rx) = mpsc::channel();
    let pool = ThreadPool::new(if n_jobs <= 1 { 1 } else { n_jobs / 2 });
    */
    let num_elements = 1025;

    let kvs = amphis::kvs::KVS::new();
    for i in 0..num_elements {
        let key = "k".to_string() + &i.to_string();
        let value = "v".to_string() + &i.to_string();
        kvs.insert(&key.as_bytes().to_vec(), &value.as_bytes().to_vec())
            .unwrap();
    }

    /*
    let kvs = Arc::new(amphis::kvs::KVS::new());

    for i in 0..n_jobs {
        let key = "k".to_string() + &i.to_string();
        let value = "v".to_string() + &i.to_string();
        let h = kvs.clone();

        let tx = tx.clone();
        pool.execute(move || {
            let result = h.insert(&key.as_bytes().to_vec(), &value.as_bytes().to_vec()).unwrap();
            tx.send(result)
                .expect("channel will be there waiting for the pool");
        });
    }
    assert_eq!(rx.iter().take(n_jobs).all(|r| r == ()), true);
    */

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
