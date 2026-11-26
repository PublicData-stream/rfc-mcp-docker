//! Immutable original-byte storage and PostgreSQL read access.

pub mod blob;
pub mod manifest;
pub mod policy;
pub mod postgres;

/// The dedicated Compose fixture binary can pause or terminate at durable boundaries.
#[doc(hidden)]
pub fn fixture_boundary(name: &str) {
    #[cfg(feature = "phase3-fixtures")]
    {
        use std::io::Write;
        if std::env::var("RFC_PHASE3_CRASH").as_deref() == Ok(name) {
            eprintln!("crash at {name}");
            std::process::exit(86);
        }
        if std::env::var("RFC_PHASE3_PAUSE").as_deref() == Ok(name) {
            println!("paused {name}");
            std::io::stdout().flush().expect("flush fixture checkpoint");
            assert!(
                std::io::stdin()
                    .read_line(&mut String::new())
                    .expect("release fixture checkpoint")
                    > 0
            );
        }
    }
    #[cfg(not(feature = "phase3-fixtures"))]
    let _ = name;
}
