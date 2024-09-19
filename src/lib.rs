#![deny(clippy::all)]
#![feature(mapped_lock_guards)]
#![feature(inline_const_pat)]
#![feature(let_chains)]
// TODO: It may be better if this were enabled, but we would need to go through and figure out how to stop the false positives.
#![allow(unused)]
#![deny(unused_must_use)]
#![warn(unused_variables)]
#![warn(unused_imports)]

use engine::SyncVoiceEngine;

#[allow(unused_imports)]
pub(crate) use parking_lot::{
	MappedMutexGuard as MappedSyncMutexGuard, Mutex as SyncMutex, MutexGuard as SyncMutexGuard,
};

#[allow(unused_imports)]
pub(crate) use parking_lot::{MappedReentrantMutexGuard, ReentrantMutex, ReentrantMutexGuard};

#[allow(unused_imports)]
pub(crate) use tokio::sync::{MappedMutexGuard as MappedAsyncMutexGuard, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

mod engine;
use engine::VoiceEngine;

mod connection;

mod drains;

use napi::bindgen_prelude::*;
use napi::{JsObject, Result};
use napi_derive::module_exports;

#[module_exports]
fn init(_exports: JsObject, env: Env) -> Result<()> {
	let engine = VoiceEngine::new(env)?;
	env.set_instance_data(SyncVoiceEngine::new(engine), (), |finalize_context| {
		drop(finalize_context.value)
	})
	.map_err(|_| napi::Error::from_reason("Failed to set instance data"))?;

	Ok(())
}
