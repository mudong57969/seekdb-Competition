use crate::error::Error;
use crate::schema::{Cell, Schema};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub type Row = Vec<Cell>;
type Reader = BufReader<File>;
pub type Writer = BufWriter<File>;
const BUFFER: usize = 64 * 1024;

pub struct Storage {
    root: PathBuf,
    _lock: File,
    active: bool,
    poisoned: bool,
    #[cfg(test)]
    temporary: bool,
}

impl Storage {
    pub fn open(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        lock.try_lock().map_err(io::Error::from)?;
        let storage = Self {
            root,
            _lock: lock,
            active: false,
            poisoned: false,
            #[cfg(test)]
            temporary: false,
        };
        storage.install()?;
        discard_directory(&storage.root.join("pending"))?;
        match fs::remove_file(storage.root.join("statement.tmp")) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(storage)
    }

    fn install(&self) -> io::Result<()> {
        let committed = self.root.join("committed");
        let entries = match fs::read_dir(&committed) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        // Each rename consumes one entry. After a kill the remaining entries
        // are sufficient, and readers are not admitted until installation ends.
        for entry in entries {
            let entry = entry?;
            fs::rename(entry.path(), self.root.join(entry.file_name()))?;
        }
        sync_directory(&self.root)?;
        fs::remove_dir(committed)?;
        sync_directory(&self.root)
    }

    pub fn ensure_available(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    pub fn begin(&mut self) -> Result<(), Error> {
        if self.active {
            return Err(Error::TransactionState);
        }
        fs::create_dir(self.root.join("pending"))?;
        self.active = true;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if !self.active {
            return Err(Error::TransactionState);
        }
        let pending = self.root.join("pending");
        self.poisoned = true;
        sync_directory(&pending)?;
        fs::rename(pending, self.root.join("committed"))?;
        self.active = false;
        sync_directory(&self.root)?;
        self.install()?;
        self.poisoned = false;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        if !self.active {
            return Err(Error::TransactionState);
        }
        self.poisoned = true;
        discard_directory(&self.root.join("pending"))?;
        self.active = false;
        self.poisoned = false;
        Ok(())
    }

    pub fn write_statement<T>(
        &mut self,
        write: impl FnOnce(&Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let auto = !self.active;
        if auto {
            self.begin()?;
        }
        let result = write(self);
        if auto {
            if result.is_ok() {
                self.commit()?;
            } else {
                self.rollback()?;
            }
        }
        result
    }

    pub fn create(&self, name: &str, schema: &Schema) -> Result<(), Error> {
        match self.table(name) {
            Err(Error::UnknownTable) => self.rewrite(name, schema, |_| Ok(())),
            Err(error) => Err(error),
            Ok(_) => Err(Error::TableExists),
        }
    }

    pub fn table(&self, name: &str) -> Result<Table, Error> {
        let filename = filename(name);
        let staged = self.root.join("pending").join(&filename);
        let (path, file) = match File::open(&staged) {
            Ok(file) => (staged, file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let path = self.root.join(filename);
                match File::open(&path) {
                    Ok(file) => (path, file),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        return Err(Error::UnknownTable)
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let mut reader = BufReader::with_capacity(BUFFER, file);
        let mut line = Vec::new();
        let schema = read_record(&mut reader, &mut line)?.ok_or(Error::Storage)?;
        Ok(Table { path, schema })
    }

    pub fn rewrite<T>(
        &self,
        name: &str,
        schema: &Schema,
        write: impl FnOnce(&mut Writer) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let scratch = self.root.join("statement.tmp");
        let mut writer = BufWriter::with_capacity(BUFFER, File::create(&scratch)?);
        write_record(&mut writer, schema)?;
        let result = write(&mut writer)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(scratch, self.root.join("pending").join(filename(name)))?;
        Ok(result)
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        if self.active && !self.poisoned {
            let _ = self.rollback();
        }
        #[cfg(test)]
        if self.temporary {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

#[cfg(test)]
impl Default for Storage {
    fn default() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        loop {
            let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let root =
                std::env::temp_dir().join(format!("minivectordb-{}-{id}", std::process::id()));
            match fs::create_dir(&root) {
                Ok(()) => {
                    let mut storage = Self::open(root).unwrap();
                    storage.temporary = true;
                    return storage;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("{error}"),
            }
        }
    }
}

fn filename(name: &str) -> String {
    format!("table-{}.jsonl", name.to_ascii_lowercase())
}

fn discard_directory(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub struct Table {
    path: PathBuf,
    pub schema: Schema,
}

impl Table {
    pub fn scan(&self) -> Result<impl Iterator<Item = Result<Row, Error>>, Error> {
        let mut reader = BufReader::with_capacity(BUFFER, File::open(&self.path)?);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line)?;
        Ok(std::iter::from_fn(move || {
            read_record(&mut reader, &mut line).transpose()
        }))
    }

    pub fn scan_where<'a>(
        &self,
        mut include: impl FnMut(&[u8]) -> Result<bool, Error> + 'a,
    ) -> Result<impl Iterator<Item = Result<Row, Error>> + 'a, Error> {
        let mut reader = BufReader::with_capacity(BUFFER, File::open(&self.path)?);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line)?;
        Ok(std::iter::from_fn(move || loop {
            line.clear();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) => return None,
                Err(error) => return Some(Err(error.into())),
                Ok(_) => {}
            }
            match include(&line) {
                Ok(false) => continue,
                Ok(true) => return Some(decode_record(&line)),
                Err(error) => return Some(Err(error)),
            }
        }))
    }

    pub fn key(&self, row: &Row) -> Result<i64, Error> {
        match row.get(self.schema.primary) {
            Some(Cell::BigInt(key)) => Ok(*key),
            _ => Err(Error::Storage),
        }
    }
}

fn read_record<T: DeserializeOwned>(
    reader: &mut Reader,
    line: &mut Vec<u8>,
) -> Result<Option<T>, Error> {
    line.clear();
    if reader.read_until(b'\n', line)? == 0 {
        return Ok(None);
    }
    decode_record(line).map(Some)
}

pub fn decode_record<'a, T: serde::Deserialize<'a>>(line: &'a [u8]) -> Result<T, Error> {
    serde_json::from_slice(line).map_err(|error| {
        eprintln!("Snapshot decode failed: {error}");
        Error::Storage
    })
}

pub fn write_record(writer: &mut Writer, value: &impl Serialize) -> Result<(), Error> {
    serde_json::to_writer(&mut *writer, value).map_err(io::Error::from)?;
    writer.write_all(b"\n")?;
    Ok(())
}
