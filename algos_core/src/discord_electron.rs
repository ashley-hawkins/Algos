use std::{
	ffi::{CStr, CString},
	os::raw::{c_char, c_void},
};
#[cfg(target_os = "windows")]
use windows::Win32::Foundation::HANDLE;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DiscordYUVFrame {
	pub y: *const u8,
	pub u: *const u8,
	pub v: *const u8,
	pub y_stride: i32,
	pub u_stride: i32,
	pub v_stride: i32,
}

impl DiscordYUVFrame {
	pub unsafe fn from_raw_unchecked(data: *const u8, width: i32, height: i32) -> Self {
		Self {
			y: data,
			u: data.add((width * height) as usize),
			v: data.add((width * height * 5 / 4) as usize),
			y_stride: width,
			u_stride: width / 2,
			v_stride: width / 2,
		}
	}
}

#[repr(i32)]
pub enum DiscordFrameType {
	#[cfg(target_os = "windows")]
	DiscordFrameNative = 0,
	DiscordFrameI420 = 1,
}

#[repr(C)]
pub union DiscordFrameUnion {
	pub yuv: DiscordYUVFrame,
	#[cfg(target_os = "windows")]
	pub texture_handle: HANDLE,
}

#[repr(C)]
pub struct DiscordFrame {
	pub timestamp_us: i64,
	pub frame: DiscordFrameUnion,
	pub width: i32,
	pub height: i32,
	pub type_: DiscordFrameType,
}

type DiscordFrameReleaseCB = extern "C" fn(*mut c_void);

extern "C" {
	pub fn DeliverDiscordFrame(
		stream_id: *const c_char,
		frame: *mut DiscordFrame,
		release_cb: *const DiscordFrameReleaseCB,
		user_data: *mut c_void,
	);
}

pub unsafe fn deliver_discord_frame<F>(
	stream_id: &str,
	mut frame: DiscordFrame,
	cb: F,
	user_data: *mut c_void,
) where
	F: FnOnce() + Send + 'static,
{
	type DynF = dyn FnOnce() + Send;

	extern "C" fn callback(data: *mut c_void) {
		let cb = unsafe { Box::from_raw(data as *mut Box<DynF>) };
		cb();
	}

	let stream_id = CString::new(stream_id).unwrap();

	let cb = Box::into_raw(Box::new(Box::new(cb) as Box<DynF>));

	unsafe {
		DeliverDiscordFrame(
			stream_id.as_ptr(),
			&mut frame as *mut DiscordFrame,
			callback as *const DiscordFrameReleaseCB,
			cb as *mut c_void,
		);
	}
}
