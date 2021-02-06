use serde::Deserialize;

const CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Deserialize)]
pub struct Config {
    data_dir: String,
    bloom_filter_size: usize,
}

impl Config {
    pub fn new() -> Self {
        let string = match std::fs::read_to_string(CONFIG_FILE) {
            Ok(s) => s,
            Err(e) => panic!("{}", e),
        };
        let config: Config = toml::from_str(&string).unwrap();

        config
    }

    pub fn new_with_str(s: &str) -> Self {
        let config: Config = toml::from_str(s).unwrap();
        config
    }

    pub fn get_data_dir_path(&self, name: &str) -> String {
        format!("{}/{}", self.data_dir, name)
    }

    pub fn get_leaf_file_path(&self, name: &str, id: usize) -> String {
        format!("{}/{}-{}.amph", self.get_data_dir_path(name), "leaves", id)
    }

    pub fn get_table_file_path(&self, name: &str, id: usize) -> String {
        format!("{}/sstable-{}.amph", self.get_data_dir_path(name), id)
    }

    pub fn get_bloom_filter_size(&self) -> usize {
        self.bloom_filter_size
    }
}
