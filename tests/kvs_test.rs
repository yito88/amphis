extern crate amphis;
use amphis::config::Config;
use amphis::kvs::KVS;
use env_logger;
use std::sync::{mpsc, Arc};
use threadpool::ThreadPool;

#[test]
fn test_mutations() {
    let _ = env_logger::builder().is_test(true).try_init();
    const NUM_INSERTION: usize = 1025;
    const TABLE_NAME: &str = "mutation_test";
    let config = Config::new();
    let kvs = KVS::new(TABLE_NAME, config.clone()).unwrap();

    // INSERT
    for i in 0..NUM_INSERTION {
        let key = ("k".to_string() + &i.to_string()).as_bytes().to_vec();
        let value = ("v".to_string() + &i.to_string()).as_bytes().to_vec();
        kvs.put(&key, &value).unwrap();
    }

    // UPDATE or DELETE
    for i in 0..NUM_INSERTION {
        if i % 2 != 0 && i % 3 != 0 {
            continue;
        }

        let key = ("k".to_string() + &i.to_string()).as_bytes().to_vec();
        if i % 3 == 0 {
            kvs.delete(&key).unwrap();
        } else {
            let value = ("new-v".to_string() + &i.to_string()).as_bytes().to_vec();
            kvs.put(&key, &value).unwrap();
        }
    }

    // CHECK
    for i in 0..NUM_INSERTION {
        let key = ("k".to_string() + &i.to_string()).as_bytes().to_vec();
        let expected = if i % 2 == 0 {
            ("new-v".to_string() + &i.to_string()).as_bytes().to_vec()
        } else {
            ("v".to_string() + &i.to_string()).as_bytes().to_vec()
        };

        let actual = kvs.get(&key).unwrap();

        if i % 3 == 0 {
            assert_eq!(actual, None);
        } else {
            assert_eq!(actual.unwrap(), expected);
        }
    }

    let _ = std::fs::remove_dir_all(format!("data/{}", TABLE_NAME));
}

#[test]
fn test_recovery() {
    let _ = env_logger::builder().is_test(true).try_init();
    const NUM_INSERTION: usize = 1025;
    const TABLE_NAME: &str = "recovery_test";
    let config = Config::new();
    let kvs = KVS::new(TABLE_NAME, config.clone()).unwrap();

    // INSERT
    for i in 0..NUM_INSERTION {
        let key = "k".to_string() + &i.to_string();
        let value = "v".to_string() + &i.to_string();
        kvs.put(&key.as_bytes().to_vec(), &value.as_bytes().to_vec())
            .unwrap();
    }

    // RESTART
    drop(kvs);
    let kvs = KVS::new(TABLE_NAME, config.clone()).unwrap();

    // CHECK
    for i in 0..NUM_INSERTION {
        let key = format!("{}{}", "k", (&*i.to_string()));
        let expected = format!("{}{}", "v", (&*i.to_string())).as_bytes().to_vec();

        let actual = kvs
            .get(&key.as_bytes().to_vec())
            .expect("read failed")
            .expect("no value");

        assert_eq!(actual, expected);
    }

    let _ = std::fs::remove_dir_all(format!("data/{}", TABLE_NAME));
}

#[test]
fn concurrent_insert() {
    let _ = env_logger::builder().is_test(true).try_init();
    const NUM_INSERTION: usize = 1025;
    const NUM_THREADS: usize = 4;
    const TABLE_NAME: &str = "concurrency_test";
    let config = Config::new();
    let kvs = Arc::new(amphis::kvs::KVS::new(TABLE_NAME, config).expect("failed to start Amphis"));

    let (tx, rx) = mpsc::channel();
    let pool = ThreadPool::new(if NUM_THREADS <= 1 { 1 } else { NUM_THREADS });

    for i in 0..NUM_THREADS {
        let each = kvs.clone();

        let tx = tx.clone();
        pool.execute(move || {
            for v in 0..NUM_INSERTION {
                let key = format!("k{}:{}", v, i).as_bytes().to_vec();
                let value = format!("v{}:{}", v, i).as_bytes().to_vec();
                each.put(&key, &value).unwrap();
            }
            let result = 0;
            tx.send(result)
                .expect("channel will be there waiting for the pool");
        });
    }

    assert_eq!(rx.iter().take(NUM_THREADS).all(|r| r == 0), true);

    for i in 0..NUM_THREADS {
        let each = kvs.clone();

        let tx = tx.clone();
        pool.execute(move || {
            for v in 0..NUM_INSERTION {
                let key = format!("k{}:{}", v, i);
                let expected = format!("v{}:{}", v, i);
                match each.get(&key.as_bytes().to_vec()).unwrap() {
                    Some(value) => {
                        let actual = String::from_utf8(value.to_vec()).unwrap();
                        assert_eq!(actual, expected);
                    }
                    None => panic!("expected: {}, actual: None", expected),
                };
            }
            let result = 0;
            tx.send(result)
                .expect("channel will be there waiting for the pool");
        });
    }

    assert_eq!(rx.iter().take(NUM_THREADS).all(|r| r == 0), true);

    let _ = std::fs::remove_dir_all(format!("data/{}", TABLE_NAME));
}
