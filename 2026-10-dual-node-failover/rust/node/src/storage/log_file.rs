use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use shared::state_file::{create_parent_dir, temp_path};

use super::LogEntry;

#[derive(Debug)]
pub enum LogFileError {
    Io(io::Error),
    /// A bad line before the end: not something a killed process can leave behind.
    Corrupt {
        path: PathBuf,
        line: usize,
    },
}

impl fmt::Display for LogFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Corrupt { path, line } => write!(f, "{} line {line} is corrupt", path.display()),
        }
    }
}

impl std::error::Error for LogFileError {}

impl From<io::Error> for LogFileError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// JSON-lines file behind the replicated log, one entry per line. Every append is written to the OS
/// before it returns, so a killed process loses nothing it acknowledged. Power loss is out of scope:
/// there is no fsync.
pub struct LogFile {
    path: PathBuf,
    file: File,
}

impl LogFile {
    /// Opens (or creates) the file and returns the entries in it. A torn last line, which is all a
    /// process killed mid-write can leave, is dropped; anything wrong before that is refused.
    pub fn open(path: &Path) -> Result<(Self, Vec<LogEntry>), LogFileError> {
        create_parent_dir(path)?;
        let mut entries: Vec<LogEntry> = Vec::new();
        match fs::read(path) {
            Ok(bytes) => {
                let lines = split_lines(&bytes);
                for (i, line) in lines.iter().enumerate() {
                    if let Some(entry) = parse_line(line)
                        && entry.index == entries.len() as i64 + 1
                    {
                        entries.push(entry);
                        continue;
                    }
                    if i + 1 != lines.len() {
                        return Err(LogFileError::Corrupt {
                            path: path.to_owned(),
                            line: i + 1,
                        });
                    }
                }
                // Every append ends in a newline. Without one the next append would be glued onto
                // the last line, so rewrite the file: drops a torn line, completes a whole one.
                if entries.len() != lines.len() || bytes.last().is_some_and(|b| *b != b'\n') {
                    write_all(path, &entries)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let file = open_for_append(path)?;
        Ok((
            Self {
                path: path.to_owned(),
                file,
            },
            entries,
        ))
    }

    pub fn append(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        let bytes = serialize(entries)?;
        let end = self.file.metadata()?.len();
        self.file.write_all(&bytes).inspect_err(|_| {
            // Cut off whatever part made it to disk, so the next append does not land after half
            // a line. If this fails too, the write error is still the one to report.
            let _ = self.file.set_len(end);
        })
    }

    /// Replaces the whole file. Used when a rejoining node drops entries the new primary never had.
    /// The new handle is opened on the temp file before the rename, so a failure leaves the old
    /// file and handle in place instead of a handle to an unlinked file.
    pub fn rewrite(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        let temp = temp_path(&self.path);
        fs::write(&temp, serialize(entries)?)?;
        let file = open_for_append(&temp)?;
        fs::rename(&temp, &self.path)?;
        self.file = file;
        Ok(())
    }
}

fn open_for_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn write_all(path: &Path, entries: &[LogEntry]) -> io::Result<()> {
    let temp = temp_path(path);
    fs::write(&temp, serialize(entries)?)?;
    fs::rename(&temp, path)
}

fn serialize(entries: &[LogEntry]) -> io::Result<Vec<u8>> {
    let mut text = Vec::with_capacity(entries.len() * 64);
    for entry in entries {
        serde_json::to_writer(&mut text, entry).map_err(io::Error::other)?;
        text.push(b'\n');
    }
    Ok(text)
}

/// Lines without their terminator. A final newline does not start another (empty) line.
fn split_lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut lines: Vec<&[u8]> = bytes
        .split(|b| *b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect();
    if lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    lines
}

/// The line as an entry, or `None` for anything else (including a line cut in the middle of a character).
fn parse_line(line: &[u8]) -> Option<LogEntry> {
    serde_json::from_slice(line).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE_1: &str = r#"{"index":1,"epoch":1,"key":"a","value":"1"}"#;

    fn entry(index: i64) -> LogEntry {
        LogEntry {
            index,
            epoch: 1,
            key: format!("k{index}"),
            value: "v".into(),
        }
    }

    #[test]
    fn appends_one_json_line_per_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub/log.jsonl");
        let (mut file, entries) = LogFile::open(&path).unwrap();
        assert!(entries.is_empty());

        file.append(&[entry(1), entry(2)]).unwrap();

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "{\"index\":1,\"epoch\":1,\"key\":\"k1\",\"value\":\"v\"}\n{\"index\":2,\"epoch\":1,\"key\":\"k2\",\"value\":\"v\"}\n"
        );
        assert_eq!(LogFile::open(&path).unwrap().1, vec![entry(1), entry(2)]);
    }

    #[test]
    fn rewrite_replaces_the_file_and_keeps_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        let (mut file, _) = LogFile::open(&path).unwrap();
        file.append(&[entry(1), entry(2), entry(3)]).unwrap();

        file.rewrite(&[entry(1)]).unwrap();
        file.append(&[entry(2)]).unwrap();

        assert_eq!(LogFile::open(&path).unwrap().1, vec![entry(1), entry(2)]);
    }

    #[test]
    fn drops_a_torn_last_line_even_mid_character() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        let mut torn =
            format!("{LINE_1}\n{{\"index\":2,\"epoch\":1,\"key\":\"b\",\"value\":\"").into_bytes();
        torn.extend_from_slice(&"é".as_bytes()[..1]);
        fs::write(&path, torn).unwrap();

        let (_, entries) = LogFile::open(&path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), format!("{LINE_1}\n"));
    }

    #[test]
    fn refuses_corruption_before_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        for content in [
            format!("garbage\n{LINE_1}\n"),
            format!("{LINE_1}\n\n{LINE_1}\n"),
            format!("{LINE_1}\n{LINE_1}\n{LINE_1}\n"),
        ] {
            fs::write(&path, content).unwrap();
            let error = LogFile::open(&path)
                .err()
                .expect("corrupt log must not open");
            assert!(error.to_string().contains("is corrupt"), "{error}");
        }
    }

    #[test]
    fn completes_a_last_line_that_lost_only_its_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        fs::write(&path, LINE_1).unwrap();

        let (mut file, entries) = LogFile::open(&path).unwrap();
        assert_eq!(entries.len(), 1);
        file.append(&[entry(2)]).unwrap();

        assert_eq!(LogFile::open(&path).unwrap().1.len(), 2);
    }

    #[test]
    fn accepts_crlf_and_a_missing_final_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        fs::write(
            &path,
            format!(
                "{LINE_1}\r\n{}",
                LINE_1.replace("\"index\":1", "\"index\":2")
            ),
        )
        .unwrap();

        assert_eq!(LogFile::open(&path).unwrap().1.len(), 2);
    }
}
