use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrudError {
    #[error("read error")]
    ReadError,
    #[error("write error")]
    WriteError,
    #[error("timed out")]
    TimedOut,
    #[error("unknown data store error")]
    Unknown,
}
