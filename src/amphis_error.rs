use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrudError {
    #[error("timed out")]
    TimedOut,
    #[error("unknown data store error")]
    Unknown,
}
