#![deny(unsafe_op_in_unsafe_fn)]

use std::{ffi::CString, os::raw::c_void};

pub struct YuvInfo {
	pub y_offset: usize,
	pub u_offset: usize,
	pub v_offset: usize,
	pub y_stride: i32,
	pub u_stride: i32,
	pub v_stride: i32,
}

impl YuvInfo {
	fn into_raw<T: AsRef<[u8]> + 'static>(self, owned_memory: &T) -> discord_electron_ffi::DiscordYuvFrame {
		let memory_slice = owned_memory.as_ref();

		if self.y_offset >= memory_slice.len()
			|| self.u_offset >= memory_slice.len()
			|| self.v_offset >= memory_slice.len()
		{
			panic!("YUV offsets are out of bounds");
		}

		let ptr = memory_slice.as_ptr();

		discord_electron_ffi::DiscordYuvFrame {
			y: unsafe { ptr.add(self.y_offset) },
			u: unsafe { ptr.add(self.u_offset) },
			v: unsafe { ptr.add(self.v_offset) },
			y_stride: self.y_stride,
			u_stride: self.u_stride,
			v_stride: self.v_stride,
		}
	}
}

pub struct DiscordYuvFrame<T: AsRef<[u8]>>(T, YuvInfo);

impl<T: AsRef<[u8]> + 'static> DiscordYuvFrame<T> {
	pub fn new(memory: T, width: i32, height: i32) -> Self {
		Self(
			memory,
			YuvInfo {
				y_offset: 0,
				u_offset: (width * height) as usize,
				v_offset: (width * height * 5 / 4) as usize,
				y_stride: width as i32,
				u_stride: width as i32 / 2,
				v_stride: width as i32 / 2,
			},
		)
	}

	pub fn into_parts(self) -> (T, YuvInfo) {
		(self.0, self.1)
	}
}

pub struct DiscordFrame<T: AsRef<[u8]> + 'static> {
	pub timestamp_us: i64,
	pub frame: DiscordYuvFrame<T>,
	pub width: i32,
	pub height: i32,
}

impl<T: AsRef<[u8]> + 'static> DiscordFrame<T> {
	fn into_raw(self) -> (T, discord_electron_ffi::DiscordFrame) {
		let (owned_memory, yuv_info) = self.frame.into_parts();

		let raw_frame = discord_electron_ffi::DiscordFrame {
			timestamp_us: self.timestamp_us,
			frame: discord_electron_ffi::DiscordFrame__bindgen_ty_1 {
				yuv: yuv_info.into_raw(&owned_memory),
			},
			width: self.width,
			height: self.height,
			type_: discord_electron_ffi::DiscordFrameType_DISCORD_FRAME_I420 as i32,
		};

		(owned_memory, raw_frame)
	}
}

// Unsafe because you could easily pass in an invalid yuv frame causing it to read out of bounds...
// TODO: make it actually check everything so it's safe
pub unsafe fn deliver_discord_frame<T: AsRef<[u8]>>(stream_id: &str, frame: DiscordFrame<T>) {
	let (owned_mem, raw_frame) = frame.into_raw();
	extern "C" fn release_cb<T>(data: *mut c_void) {
		let owned_mem = unsafe { Box::from_raw(data as *mut T) };
		drop(owned_mem);
	}

	let stream_id_c_string = CString::new(stream_id).unwrap();
	unsafe {
		discord_electron_ffi::DeliverDiscordFrame(
			stream_id_c_string.as_ptr(),
			(&raw_frame) as _,
			Some(release_cb::<T>),
			Box::into_raw(Box::new(owned_mem)) as _,
		);
	}
}
