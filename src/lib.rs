#![deny(clippy::all)]
#![feature(mapped_lock_guards)]
#![feature(inline_const_pat)]
#![feature(let_chains)]
#![feature(panic_backtrace_config)]
#![feature(panic_payload_as_str)]
#![feature(random)]
// TODO: It may be better if this were enabled, but we would need to go through and figure out how to stop the false positives.
#![allow(unused)]
#![deny(unused_must_use)]
#![warn(unused_variables)]
#![warn(unused_imports)]

use engine::SyncVoiceEngine;

pub(crate) use algos_core::*;

mod engine;
use engine::VoiceEngine;

mod js_voice_connection;

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
