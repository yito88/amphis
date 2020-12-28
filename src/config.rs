use serde::Deserialize;

const CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Deserialize)]
pub struct Config {
    pub data_dir: String,
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
