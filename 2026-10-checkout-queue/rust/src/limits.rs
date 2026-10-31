//! Each waiting user may hold an event stream plus a polling connection, so one instance can need
//! thousands of sockets. Linux often starts processes at 1024 open files and macOS at 256; unlike
//! the .NET runtime, Rust's standard library does not raise that, so the instance does it itself.

/// Open files to ask for: enough for a few thousand users per instance.
pub const WANTED_OPEN_FILES: u64 = 10_240;

/// Raises the soft open-file limit to [`WANTED_OPEN_FILES`] (never above the hard limit, never
/// lowers it). Returns the limit in effect afterwards.
pub fn raise_open_file_limit() -> std::io::Result<u64> {
    rlimit::increase_nofile_limit(WANTED_OPEN_FILES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raises_the_soft_limit_as_far_as_the_hard_limit_allows() {
        let (_, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap();

        let now = raise_open_file_limit().unwrap();

        let (soft, _) = rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap();
        assert_eq!(soft, now);
        assert!(
            now >= WANTED_OPEN_FILES.min(hard),
            "soft limit {now} is below {WANTED_OPEN_FILES}"
        );
    }
}
