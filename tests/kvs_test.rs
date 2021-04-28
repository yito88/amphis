extern crate amphis;
use amphis::config::Config;
use amphis::kvs::KVS;
use env_logger;

#[test]
fn test_mutations() {
    let _ = env_logger::builder().is_test(true).try_init();
    const NUM_INSERTION: usize = 1025;
    const TABLE_NAME: &str = "mutation_test";
    let conf = "data_dir = 'tests'\nbloom_filter_size = 32768";
    let config = Config::new_with_str(conf);
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

    let _ = std::fs::remove_dir_all(format!("tests/{}", TABLE_NAME));
}

#[test]
fn test_recovery() {
    let _ = env_logger::builder().is_test(true).try_init();
    const NUM_INSERTION: usize = 1025;
    const TABLE_NAME: &str = "recovery_test";
    let conf = "data_dir = 'tests'\nbloom_filter_size = 32768";
    let config = Config::new_with_str(conf);
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

    let _ = std::fs::remove_dir_all(format!("tests/{}", TABLE_NAME));
}
