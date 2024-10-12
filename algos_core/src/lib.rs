#![feature(inline_const_pat)]
#![feature(random)]
#![feature(let_chains)]

pub mod constants;
pub mod crypt;
pub mod discord_electron;
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

pub fn add(left: u64, right: u64) -> u64 {
	left + right
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn it_works() {
		let result = add(2, 2);
		assert_eq!(result, 4);
	}
}
