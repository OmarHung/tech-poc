/// One line per event on stdout, with a UTC timestamp, e.g.
/// `2026-10-31T01:02:03.456789Z  WARN Promoted to primary in epoch 2 at index 0; standby in sync: false`.
pub fn init() {
    // try_init: a second call (tests) keeps the first subscriber instead of panicking.
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .try_init();
}
