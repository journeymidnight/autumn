use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("not leader")]
    NotLeader,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("precondition failed: {0}")]
    Precondition(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    Internal(String),
}
