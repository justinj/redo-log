use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
#[cfg(test)]
use tempfile::tempdir;

#[derive(Debug)]
struct DbInner {
    log: File,
    memtable: HashMap<String, String>,
}

unsafe impl Send for DbInner {}

#[derive(Debug, Clone)]
struct Db {
    inner: Arc<Mutex<DbInner>>,
}

unsafe impl Send for Db {}

#[derive(Serialize, Deserialize)]
enum Command<'a> {
    Set(&'a str, &'a str),
    Delete(&'a str),
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
            inner: Arc::new(Mutex::new(DbInner { log, memtable })),
        })
    }

    fn apply_command_to_memtable(memtable: &mut HashMap<String, String>, cmd: &Command) {
        match cmd {
            Command::Set(k, v) => {
                memtable.insert((*k).to_owned(), (*v).to_owned());
            }
            Command::Delete(k) => {
                memtable.remove(*k);
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
        let mut inner = self.inner.lock().unwrap();
        inner.log.write_all(&serde_json::to_vec(command)?)?;
        inner.log.write_all(b"\n")?;
        inner.log.sync_all()?;
        Self::apply_command_to_memtable(&mut inner.memtable, command);
        Ok(())
    }

    fn set(&mut self, k: &str, v: &str) -> Result<()> {
        self.apply_command(&Command::Set(k, v))?;
        Ok(())
    }

    fn delete(&mut self, k: &str) -> Result<()> {
        self.apply_command(&Command::Delete(k))?;
        Ok(())
    }

    fn get(&self, k: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        inner.memtable.get(k).cloned()
    }
}

fn main() -> Result<()> {
    let db = Db::new("logfile")?;

    for i in 0..2 {
        let mut db = db.clone();
        thread::spawn(move || {
            for j in 0..5 {
                db.set(
                    format!("key{}_{}", j, i).as_str(),
                    format!("val{}_{}", j, i).as_str(),
                )
                .unwrap();
            }
        });
    }

    // Give those threads a chance to finish...
    thread::sleep(Duration::from_millis(100));

    for i in 0..2 {
        for j in 0..5 {
            let key = format!("key{}_{}", j, i);
            println!("{} = {:?}", key, db.get(key.as_str()));
        }
    }

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
