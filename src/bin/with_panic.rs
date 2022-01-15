use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
#[cfg(test)]
use tempfile::tempdir;

mod panic_kernel;

#[derive(Debug)]
struct Db {
    log: File,
    fname: PathBuf,
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
        Ok(Db {
            log,
            fname: f.as_ref().to_path_buf(),
        })
    }

    fn apply_command(&mut self, command: &Command) -> Result<()> {
        self.log.write_all(&serde_json::to_vec(command)?)?;
        self.log.write_all(b"\n")?;
        self.log.sync_all()?;
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

    fn get(&self, k: &str) -> Result<Option<String>> {
        let file = BufReader::new(File::open(&self.fname)?);
        let mut result = None;
        for line in file.lines() {
            match serde_json::from_str(&line?)? {
                Command::Set(new_k, v) => {
                    if k == new_k {
                        result = Some(v.to_owned());
                    }
                }
                Command::Delete(new_k) => {
                    if k == new_k {
                        result = None;
                    }
                }
            }
        }
        Ok(result)
    }
}

fn main() -> Result<()> {
    panic_kernel::panic_kernel();
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
    assert_eq!(db.get("foo")?, Some("bar".into()));
    db.delete("foo")?;
    assert_eq!(db.get("foo")?, None);

    Ok(())
}

#[test]
fn test_basic_recover() -> Result<()> {
    let dir = tempdir()?;
    let file = dir.path().to_path_buf().join("logfile");

    let mut db = Db::new(&file)?;

    let t = Instant::now();
    let mut writes = 0;
    while Instant::now().duration_since(t) < Duration::from_millis(10000) {
        writes += 1;
        db.set("foo", "bar").unwrap();
    }
    println!("performed {} writes in 10 seconds", writes);

    db.set("foo", "bar")?;
    db.set("baz", "goo")?;
    assert_eq!(db.get("foo")?, Some("bar".into()));
    db.delete("foo")?;
    assert_eq!(db.get("foo")?, None);

    let db = Db::new(&file)?;
    assert_eq!(db.get("baz")?, Some("goo".into()));

    Ok(())
}
