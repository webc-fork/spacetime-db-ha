//! Raft 存储层共用的错误类型。

use std::io;

/// 存储层内部错误。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error(transparent)]
    Bitcode(#[from] bitcode::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<StoreError> for io::Error {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Io(e) => e,
            other => Self::other(other),
        }
    }
}

/// 把内部错误转成 `openraft` 0.10 存储层要求的 `std::io::Error`。
pub fn storage_io_err(err: impl Into<StoreError>) -> io::Error {
    err.into().into()
}
