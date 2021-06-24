use serde_yaml::from_reader;
use serde_yaml::Value;
use std::env;

fn load() -> Value {
    let mut home_path = String::from("/root");
    match env::var("HOME") {
        Ok(value) => home_path = value.to_owned(),
        Err(e) => println!("Couldn't read HOME ({})", e),
    };

    let f = std::fs::File::open(format!("{}/.config/fs-events/config.yml", home_path)).unwrap();
    let config_map: Value = from_reader(f).unwrap();
    return config_map;
}

fn filesystem() -> Value {
    let config: Value = load();
    let filesystem_configs = config.get("filesystem").unwrap();
    return filesystem_configs.clone();
}

pub fn root_directories() -> std::vec::Vec<Value> {
    let filesystem_configs = filesystem();
    let git_config = filesystem_configs.get("root_directories").unwrap();
    return git_config.as_sequence().unwrap().clone();
}
