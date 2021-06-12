use serde::{Deserialize, Serialize};
use std::path::Path;

const CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    leaf_dir: String,
    table_dir: String,
    bloom_filter_items_count: usize,
    bloom_filter_fp_rate: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            leaf_dir: "data".to_owned(),
            table_dir: "data".to_owned(),
            bloom_filter_items_count: 8192,
            bloom_filter_fp_rate: 0.01,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        let mut config = config::Config::default();

        config
            .set_default("leaf_dir", "data")
            .expect("cannot parse the key");
        config
            .set_default("table_dir", "data")
            .expect("cannot parse the key");
        config
            .set_default("bloom_filter_items_count", 8192)
            .expect("cannot parse the key");
        config
            .set_default("bloom_filter_fp_rate", 0.01)
            .expect("cannot parse the key");

        if Path::new(CONFIG_FILE).exists() {
            config
                .merge(config::File::with_name(CONFIG_FILE))
                .expect("reading a config file failed");
        }
        config.try_into().expect("deserializing config failed")
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Self {
        let temp_dir = tempfile::tempdir().expect("no temp directry");
        let path_str = temp_dir.path().to_str().expect("no path");
        let mut config = config::Config::default();

        config
            .set_default("leaf_dir", path_str)
            .expect("cannot parse the key");
        config
            .set_default("table_dir", path_str)
            .expect("cannot parse the key");
        config
            .set_default("bloom_filter_items_count", 8192)
            .expect("cannot parse the key");
        config
            .set_default("bloom_filter_fp_rate", 0.01)
            .expect("cannot parse the key");

        config.try_into().expect("deserializing config failed")
    }

    pub fn get_leaf_dir_path(&self, name: &str) -> String {
        format!("{}/{}", self.leaf_dir, name)
    }

    pub fn get_table_dir_path(&self, name: &str) -> String {
        format!("{}/{}", self.table_dir, name)
    }

    pub fn get_leaf_file_path(&self, name: &str, id: usize) -> String {
        format!("{}/leaves-{}.amph", self.get_leaf_dir_path(name), id)
    }

    pub fn get_table_file_path(&self, name: &str, id: usize) -> String {
        format!("{}/sstable-{}.amph", self.get_table_dir_path(name), id)
    }

    pub fn get_filter_items_count(&self) -> usize {
        self.bloom_filter_items_count
    }

    pub fn get_filter_fp_rate(&self) -> f64 {
        self.bloom_filter_fp_rate
    }

    pub fn get_filter_file_path(&self, name: &str) -> String {
        format!("{}/filter.amph", self.get_table_dir_path(name))
    }

    pub fn get_index_file_path(&self, name: &str) -> String {
        format!("{}/index.amph", self.get_table_dir_path(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = Config::new();
        assert_eq!(config.leaf_dir, "data");
        assert_eq!(config.table_dir, "data");
        assert_eq!(config.bloom_filter_items_count, 8192);
        assert_eq!(config.bloom_filter_fp_rate, 0.01);
    }
}
