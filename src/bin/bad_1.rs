use anyhow::Result;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tempfile::tempdir;

struct Db {
    data: HashMap<String, String>,
    fname: PathBuf,
}

impl Db {
    fn new<P>(f: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let data = std::fs::read_to_string(&f)
            .and_then(|v| Ok(serde_json::from_str(v.as_str())?))
            .unwrap_or_else(|_| HashMap::new());
        Ok(Db {
            data,
            fname: f.as_ref().to_path_buf(),
        })
    }

    fn set(&mut self, k: &str, v: &str) {
        self.data.insert(k.to_owned(), v.to_owned());
    }

    fn delete(&mut self, k: &str) {
        self.data.remove(k);
    }

    fn get(&self, k: &str) -> Option<&String> {
        self.data.get(k)
    }

    fn flush(&self) -> Result<()> {
        std::fs::write(&self.fname, serde_json::to_vec(&self.data)?)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut db = Db::new("db_data")?;
    println!("value of abc is {:?}", db.get("abc"));
    db.set("abc", "def");
    println!("value of abc is {:?}", db.get("abc"));
    panic!("");

    db.flush()?;
    Ok(())
}

#[test]
fn test_basic() -> Result<()> {
    let dir = tempdir()?;
    let file = dir.path().to_path_buf().join("data");

    let mut db = Db::new(&file)?;
    db.set("foo", "bar");
    db.set("baz", "goo");
    assert_eq!(db.get("foo"), Some(&"bar".into()));
    db.delete("foo");
    assert_eq!(db.get("foo"), None);

    Ok(())
}

#[test]
fn test_recover() -> Result<()> {
    let dir = tempdir()?;
    let file = dir.path().to_path_buf().join("data");

    let mut db = Db::new(&file)?;
    db.set("foo", "bar");
    db.set("baz", "goo");
    assert_eq!(db.get("foo"), Some(&"bar".into()));
    db.delete("foo");
    assert_eq!(db.get("foo"), None);

    db.flush();

    let db = Db::new(&file)?;
    assert_eq!(db.get("baz"), Some(&"goo".into()));

    Ok(())
}
