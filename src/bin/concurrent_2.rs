use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
#[cfg(test)]
use tempfile::tempdir;

#[derive(Debug)]
struct Db {
    log: File,
    memtable: Arc<Mutex<HashMap<String, String>>>,
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Db {
            log: self.log.try_clone().unwrap(),
            memtable: self.memtable.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Command {
    Set(String, String),
    Delete(String),
}

impl Db {
    fn new<P>(f: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let log = OpenOptions::new().create(true).append(true).open(&f)?;
        log.sync_all()?;
        let memtable = Self::replay_log(&f)?;
        Ok(Db {
            log,
            memtable: Arc::new(Mutex::new(memtable)),
        })
    }

    fn apply_command_to_memtable(memtable: &mut HashMap<String, String>, cmd: &Command) {
        match cmd {
            Command::Set(k, v) => {
                memtable.insert(k.clone(), v.clone());
            }
            Command::Delete(k) => {
                memtable.remove(k);
            }
        }
    }

    fn replay_log<P>(f: P) -> Result<HashMap<String, String>>
    where
        P: AsRef<Path>,
    {
        let file = BufReader::new(File::open(f)?);
        let mut result = HashMap::new();
        for line in file.lines() {
            Self::apply_command_to_memtable(&mut result, &serde_json::from_str(line?.as_str())?);
        }
        Ok(result)
    }

    fn apply_command(&mut self, command: &Command) -> Result<()> {
        let a = Instant::now();
        let mut data = serde_json::to_vec(command)?;
        data.extend(b"\n");
        self.log.write_all(&data)?;
        let b = Instant::now();
        println!("write latency: {:?}", b.duration_since(a).as_millis());
        self.log.sync_all()?;
        let c = Instant::now();
        println!("sync latency: {:?}", c.duration_since(b).as_millis());
        Self::apply_command_to_memtable(&mut *self.memtable.lock().unwrap(), command);
        Ok(())
    }

    fn set(&mut self, k: &str, v: &str) -> Result<()> {
        self.apply_command(&Command::Set(k.to_owned(), v.to_owned()))?;
        Ok(())
    }

    fn delete(&mut self, k: &str) -> Result<()> {
        self.apply_command(&Command::Delete(k.to_owned()))?;
        Ok(())
    }

    fn get(&self, k: &str) -> Option<String> {
        self.memtable.lock().unwrap().get(k).cloned()
    }
}

fn main() -> Result<()> {
    let db = Db::new("logfile")?;

    let writes = Arc::new(AtomicU64::new(0));

    for i in 0..10 {
        let mut db = db.clone();
        let writes = writes.clone();
        thread::spawn(move || {
            let mut j = 0;
            loop {
                db.set(
                    format!("key{}_{}", j, i).as_str(),
                    format!("val{}_{}", j, i).as_str(),
                )
                .unwrap();
                j += 1;
                writes.fetch_add(1, Ordering::SeqCst);
            }
        });
    }

    // Give those threads a chance to finish...
    thread::sleep(Duration::from_millis(10000));
    println!("we did {} writes", writes.load(Ordering::SeqCst));

    // for i in 0..2 {
    //     for j in 0..5 {
    //         let key = format!("key{}_{}", j, i);
    //         println!("{} = {:?}", key, db.get(key.as_str()));
    //     }
    // }

    Ok(())
}

#[test]
fn test_basic() -> Result<()> {
    let dir = tempdir()?;
    let file = dir.path().to_path_buf().join("logfile");

    let mut db = Db::new(&file)?;
    db.set("foo", "bar")?;
    db.set("baz", "goo")?;
    assert_eq!(db.get("foo"), Some("bar".into()));
    db.delete("foo")?;
    assert_eq!(db.get("foo"), None);

    Ok(())
}

#[test]
fn test_recover() -> Result<()> {
    let dir = tempdir()?;
    let file = dir.path().to_path_buf().join("logfile");

    let mut db = Db::new(&file)?;
    db.set("foo", "bar")?;
    db.set("baz", "goo")?;
    assert_eq!(db.get("foo"), Some("bar".into()));
    db.delete("foo")?;
    assert_eq!(db.get("foo"), None);

    let db = Db::new(&file)?;
    assert_eq!(db.get("baz"), Some("goo".into()));

    Ok(())
}
