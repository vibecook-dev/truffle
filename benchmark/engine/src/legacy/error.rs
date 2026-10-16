//! Tiny error-type shim so the legacy code doesn't depend on `anyhow` directly
//! (formerly `anyhow_lite` in `main.rs`).

pub type Result<T = ()> = std::result::Result<T, Error>;

pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl<E: std::error::Error> From<E> for Error {
    fn from(e: E) -> Self {
        Error(e.to_string())
    }
}

macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::legacy::error::Error(format!($($arg)*)))
    };
}
pub(crate) use bail;
