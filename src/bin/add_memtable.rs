use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};
#[cfg(test)]
use tempfile::tempdir;

#[derive(Debug)]
struct Db {
    log: File,
    memtable: HashMap<String, String>,
}

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
        Ok(Db { log, memtable })
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
        self.log.write_all(&serde_json::to_vec(command)?)?;
        self.log.write_all(b"\n")?;
        self.log.sync_all()?;
        Self::apply_command_to_memtable(&mut self.memtable, command);
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
        self.memtable.get(k).cloned()
    }
}

fn main() -> Result<()> {
    let mut db = Db::new("logfile")?;

    db.set("foo", "a")?;
    db.set("bar", "b")?;
    db.set("baz", "c")?;
    db.delete("bar")?;

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
