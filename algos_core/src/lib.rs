#![feature(inline_const_pat)]
#![feature(random)]
#![feature(let_chains)]

pub mod constants;
pub mod crypt;
pub mod video_thread;
pub mod voice_connection;

pub use cpal;

#[allow(unused_imports)]
pub use parking_lot::{
	RwLock as SyncRwLock, RwLockReadGuard as SyncRwLockReadGuard,
	RwLockWriteGuard as SyncRwLockWriteGuard,
};

#[allow(unused_imports)]
pub use parking_lot::{
	MappedMutexGuard as MappedSyncMutexGuard, Mutex as SyncMutex, MutexGuard as SyncMutexGuard,
};

#[allow(unused_imports)]
pub use parking_lot::{MappedReentrantMutexGuard, ReentrantMutex, ReentrantMutexGuard};

#[allow(unused_imports)]
pub use tokio::sync::{
	MappedMutexGuard as MappedAsyncMutexGuard, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard,
};