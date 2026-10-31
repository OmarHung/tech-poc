//! Small JSON state files that survive a killed process.

use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug)]
pub enum StateFileError {
    Io(PathBuf, io::Error),
    Invalid(PathBuf, String),
}

impl fmt::Display for StateFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(path, error) => write!(f, "cannot read {}: {error}", path.display()),
            Self::Invalid(path, why) => write!(f, "{} is not usable: {why}", path.display()),
        }
    }
}

impl std::error::Error for StateFileError {}

/// The saved value, or `None` if the file does not exist yet.
pub fn load<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, StateFileError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(StateFileError::Io(path.to_owned(), error)),
    };
    match serde_json::from_slice::<Option<T>>(&bytes) {
        Ok(Some(value)) => Ok(Some(value)),
        Ok(None) => Err(StateFileError::Invalid(
            path.to_owned(),
            "it is empty".into(),
        )),
        Err(error) => Err(StateFileError::Invalid(path.to_owned(), error.to_string())),
    }
}

/// Writes a temp file and renames it over the old one, so a crash leaves the old or the new state, never a mix.
pub fn save<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    create_parent_dir(path)?;
    let temp = temp_path(path);
    fs::write(&temp, serde_json::to_vec(value).map_err(io::Error::other)?)?;
    fs::rename(&temp, path)
}

/// Creates the directory a file will live in.
pub fn create_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => fs::create_dir_all(dir),
        _ => Ok(()),
    }
}

/// `<path>.tmp`, next to the file it will replace (a rename is only atomic within one file system).
pub fn temp_path(path: &Path) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(".tmp");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Record {
        epoch: i64,
        holder_name: Option<String>,
    }

    #[test]
    fn saves_and_loads_through_a_rename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/state.json");
        let record = Record {
            epoch: 7,
            holder_name: Some("a".into()),
        };

        assert!(load::<Record>(&path).unwrap().is_none());
        save(&path, &record).unwrap();

        assert_eq!(load::<Record>(&path).unwrap(), Some(record));
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            r#"{"epoch":7,"holderName":"a"}"#
        );
        assert!(!temp_path(&path).exists());
    }

    #[test]
    fn refuses_files_it_cannot_understand() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        for content in ["null", "", "{\"epoch\":\"7\"}"] {
            fs::write(&path, content).unwrap();
            let error = load::<Record>(&path).unwrap_err();
            assert!(
                matches!(error, StateFileError::Invalid(..)),
                "{content}: {error}"
            );
            assert!(error.to_string().contains("state.json"));
        }
        let unreadable = load::<Record>(dir.path()).unwrap_err();
        assert!(matches!(unreadable, StateFileError::Io(..)), "{unreadable}");
    }
}
