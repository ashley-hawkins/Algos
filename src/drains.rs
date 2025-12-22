use std::io;
use std::sync::Arc;

use derivative::Derivative;
use napi::Status;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi::{
	Env,
	threadsafe_function::{ErrorStrategy, ThreadsafeFunction},
};
use slog::{Drain, Duplicate};

use crate::{SyncMutex, SyncMutexGuard};

pub trait DrainExt: Sized + Drain {
	fn combine<D: Sized + Drain>(self, other: D) -> Duplicate<Self, D> {
		Duplicate(self, other)
	}
}

impl<D: Sized + Drain> DrainExt for D {}

struct OptionalDrain<D> {
	inner: Option<D>,
}

impl<D> OptionalDrain<D> {
	fn new() -> Self {
		Self { inner: None }
	}

	fn with(drain: D) -> Self {
		Self { inner: Some(drain) }
	}

	fn set(&mut self, drain: D) {
		self.inner = Some(drain);
	}

	fn clear(&mut self) {
		self.inner = None;
	}

	fn get(&self) -> &Option<D> {
		&self.inner
	}
}

impl<D> Drain for OptionalDrain<D>
where
	D: Drain,
{
	type Ok = Option<D::Ok>;
	type Err = D::Err;

	fn log(
		&self,
		record: &slog::Record,
		logger_values: &slog::OwnedKVList,
	) -> Result<Self::Ok, Self::Err> {
		if let Some(drain) = self.get() {
			drain.log(record, logger_values).map(Some)
		} else {
			Ok(None)
		}
	}
}

#[derive(Clone)]
struct SharedDrain<D> {
	inner: Arc<SyncMutex<D>>,
}

impl<D> SharedDrain<D> {
	fn new(drain: D) -> Self {
		Self { inner: Arc::new(SyncMutex::new(drain)) }
	}

	fn get(&self) -> SyncMutexGuard<'_, D> {
		self.inner.lock()
	}
}

// struct JsDrain {
// 	env: Env,
// 	js_console: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
// }

// impl JsDrain {
// 	fn new(env: Env, console_log: napi::JsFunction) -> Self {
// 		let js_console = console_log
// 			.create_threadsafe_function(0, |ctx| {
// 				ctx.env.create_string_from_std(ctx.value).map(|x| vec![x])
// 			})
// 			.unwrap();
// 		Self { env, js_console }
// 	}
// }

// impl Drain for JsDrain {
// 	type Ok = ();
// 	type Err = napi::Error;

// 	fn log(
// 		&self,
// 		record: &slog::Record,
// 		logger_values: &slog::OwnedKVList,
// 	) -> Result<Self::Ok, Self::Err> {
// 	self.js_console.call(
// 		Ok(())
// 	}
// }

pub struct JsWriter {
	js_console: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
}

impl JsWriter {
	pub fn new(_env: Env, console_log: napi::JsFunction) -> napi::Result<Self> {
		let mut js_console = console_log.create_threadsafe_function(0, |ctx| {
			ctx.env.create_string_from_std(ctx.value).map(|x| vec![x])
		})?;

		Ok(Self { js_console })
	}
}

impl io::Write for JsWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		let message = String::from_utf8(buf.to_owned())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
		let status = self.js_console.call(message, ThreadsafeFunctionCallMode::Blocking);
		if status == Status::Ok {
			Ok(buf.len())
		} else {
			Err(io::Error::other(format!("Failed to call JS function, {status:?}")))
		}
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

pub struct OptionalWriter<W: io::Write> {
	inner: Option<W>,
}

impl<W: io::Write> OptionalWriter<W> {
	pub fn new() -> Self {
		Self { inner: None }
	}

	pub fn with(writer: W) -> Self {
		Self { inner: Some(writer) }
	}

	pub fn set(&mut self, writer: W) {
		self.inner = Some(writer);
	}

	pub fn clear(&mut self) {
		self.inner = None;
	}

	pub fn get(&self) -> &Option<W> {
		&self.inner
	}
}

impl<W: io::Write> io::Write for OptionalWriter<W> {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		if let Some(writer) = self.inner.as_mut() { writer.write(buf) } else { Ok(buf.len()) }
	}

	fn flush(&mut self) -> io::Result<()> {
		if let Some(writer) = self.inner.as_mut() { writer.flush() } else { Ok(()) }
	}
}

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct SharedWriter<W: io::Write> {
	inner: Arc<SyncMutex<W>>,
}

impl<W: io::Write> SharedWriter<W> {
	pub fn new(writer: W) -> Self {
		Self { inner: Arc::new(SyncMutex::new(writer)) }
	}

	pub fn get(&self) -> SyncMutexGuard<'_, W> {
		self.inner.lock()
	}
}

impl<W: io::Write> io::Write for SharedWriter<W> {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.get().write(buf)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.get().flush()
	}
}

pub struct ImmediateBufferedWriter<W: io::Write> {
	inner: W,
	buffer: Vec<u8>,
}

impl<W: io::Write> ImmediateBufferedWriter<W> {
	pub fn new(inner: W) -> Self {
		ImmediateBufferedWriter { inner, buffer: Vec::new() }
	}

	fn flush_buffer(&mut self) -> io::Result<()> {
		while !self.buffer.is_empty() {
			match self.inner.write(&self.buffer) {
				Ok(0) => {
					return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write data"));
				}
				Ok(n) => self.buffer.drain(..n),
				Err(e) => return Err(e),
			};
		}
		Ok(())
	}
}

impl<W: io::Write> io::Write for ImmediateBufferedWriter<W> {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		// Write remaining buffered data first
		self.flush_buffer()?;

		// Attempt to write the new data
		match self.inner.write(buf) {
			Ok(0) => Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write data")),
			Ok(n) => {
				// If not all bytes were written, store the remaining in the buffer
				if n < buf.len() {
					self.buffer.extend_from_slice(&buf[n..]);
				}
				Ok(n)
			}
			Err(e) => {
				// If there's an error, store everything in the buffer
				self.buffer.extend_from_slice(buf);
				Err(e)
			}
		}
	}

	fn flush(&mut self) -> io::Result<()> {
		self.flush_buffer()?;
		self.inner.flush()
	}
}
