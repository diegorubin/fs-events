use crate::configs;
use notify::EventKind;
use notify::{event, RecommendedWatcher, RecursiveMode, Watcher};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_yaml::{from_value, Value};
use std::path::Path;
use std::thread;

fn produce(producer: &FutureProducer, topic_name: &str, key: &str, message: &str) {
    producer.send(
        FutureRecord::to(topic_name)
            .payload(&format!("{}", message))
            .key(&format!("{}", key)),
        0,
    );
}

fn treat_file(
    filename: std::string::String,
    _root_directories: std::vec::Vec<Value>,
    producer: &FutureProducer,
) {
    produce(producer, "fs-events", &*filename, &*filename);
    println!("file changed: {:?}", filename);
}

fn receive(event: event::Event, root_directories: std::vec::Vec<Value>, producer: &FutureProducer) {
    if !event.paths.get(0).is_none() {
        let filename = event
            .paths
            .get(0)
            .unwrap()
            .clone()
            .into_os_string()
            .into_string()
            .unwrap();
        treat_file(filename.clone(), root_directories, producer);
    }
}

fn treat(event: event::Event, root_directories: std::vec::Vec<Value>, producer: &FutureProducer) {
    match event.kind {
        EventKind::Create(_) => receive(event, root_directories, producer),
        _ => {}
    }
}

fn watch<P: AsRef<Path>>(path: P, producer: FutureProducer) -> notify::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();

    let mut watcher: RecommendedWatcher = Watcher::new_immediate(move |res| tx.send(res).unwrap())?;

    watcher.watch(path, RecursiveMode::Recursive)?;

    let root_directories = configs::root_directories();

    for res in rx {
        match res {
            Ok(event) => treat(event, root_directories.clone(), &producer),
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

pub fn init() {
    let root_directories = configs::root_directories();

    for base in root_directories {
        let path: String = from_value(base.get("path").unwrap().clone()).unwrap();
        thread::spawn(move || {
            println!("watching {}", path);
            let brokers = String::from("localhost:9092");
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &*brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error");
            if let Err(e) = watch(path, producer) {
                println!("error: {:?}", e)
            }
        });
    }

    let brokers = String::from("localhost:9092");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &*brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    if let Err(e) = watch("/tmp/fs-events", producer) {
        println!("error: {:?}", e)
    }
}
