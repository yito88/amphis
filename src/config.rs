use serde::Deserialize;

const CONFIG_FILE: &str = "config.toml";

#[derive(Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub fptree_config: FPTreeConfig,
}

#[derive(Deserialize)]
pub struct FPTreeConfig {
    pub fanout: u8,
    pub num_slot: u8,
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

    pub fn get_leaf_file_path(&self) -> String {
        format!("{}{}", self.data_dir, "/leaves.amph")
    }
}
