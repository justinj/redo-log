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
    time::Duration,
};
#[cfg(test)]
use tempfile::tempdir;

#[derive(Debug)]
enum DbState {
    // Outstanding fsync, currently no leader.
    Pending {
        // This condition variable will allow us to wait for the previous batch
        // to finish committing before we go and commit our own.
        prev_batch_notif: Arc<(Mutex<bool>, std::sync::Condvar)>,
    },
    // Outstanding fsync, there is a leader.
    PendingLeader {
        // If a new thread comes along and tries to write, it will stuff its
        // write into this buffer that the leader will use when it actually does
        // its write.
        writes: Vec<Command>,
        // This will tell us when the leader has finished writing and we can
        // safely return (informing the caller that their write has been
        // committed).
        batch_notif: Arc<(Mutex<bool>, std::sync::Condvar)>,
    },
}

#[derive(Debug, Clone)]
struct Db {
    state: Arc<Mutex<DbState>>,
    log: Arc<Mutex<File>>,
    memtable: Arc<Mutex<HashMap<String, String>>>,
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
            state: Arc::new(Mutex::new(DbState::Pending {
                prev_batch_notif: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
            })),
            log: Arc::new(Mutex::new(log)),
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

    fn wait_for(cvar: Arc<(Mutex<bool>, std::sync::Condvar)>) {
        let mut started = cvar.0.lock().unwrap();
        while !*started {
            started = cvar.1.wait(started).unwrap();
        }
    }

    fn apply_command(&mut self, command: &Command) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            DbState::Pending { .. } => {
                // There's a pending batch, but no current leader. We shall
                // become the leader.
                let done = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
                let notif = if let DbState::Pending { prev_batch_notif } = std::mem::replace(
                    &mut *state,
                    DbState::PendingLeader {
                        writes: vec![command.clone()],
                        batch_notif: done.clone(),
                    },
                ) {
                    prev_batch_notif
                } else {
                    panic!("invalid");
                };
                drop(state);
                // Now wait for the previous batch to finish.
                Self::wait_for(notif);
                // Regrab the lock.
                let mut state = self.state.lock().unwrap();
                let writes = if let DbState::PendingLeader { writes, .. } = std::mem::replace(
                    &mut *state,
                    DbState::Pending {
                        prev_batch_notif: done.clone(),
                    },
                ) {
                    writes
                } else {
                    panic!("expected to still be the leader");
                };
                let mut log = self.log.lock().unwrap();
                drop(state);
                for command in &writes {
                    log.write_all(&serde_json::to_vec(command)?)?;
                    log.write_all(b"\n")?;
                }
                log.sync_all()?;
                // Now we apply each command to the memtable:
                let mut memtable = self.memtable.lock().unwrap();
                for command in &writes {
                    Self::apply_command_to_memtable(&mut *memtable, command);
                }
                // Finally, we are done. Let everyone know.
                *done.0.lock().unwrap() = true;
                done.1.notify_all();
            }
            DbState::PendingLeader {
                writes,
                batch_notif,
            } => {
                // There is already a leader, so we will push our writes into
                // the queue and then wait for the leader to tell us that the
                // batch has been synced.
                writes.push(command.clone());
                let batch_notif = batch_notif.clone();
                drop(state);
                Self::wait_for(batch_notif);
            }
        }
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

    for i in 0..8 {
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
