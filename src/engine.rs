use crate::drains::JsWriter;
use crate::{SyncMutex, SyncMutexGuard};

use cpal::traits::{DeviceTrait, HostTrait};
use napi::{
	threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode},
	tokio, Env, JsFunction, JsObject, Result,
};
use napi_derive::napi;
use napi_derive_ext::module_interface;
use slog::{info, o, Drain};

pub struct VoiceEngine {
	root_logger: slog::Logger,
	options: Option<EngineOptions>,
}

impl VoiceEngine {
	pub(crate) fn new(mut env: Env) -> Result<Self> {
		// TODO: Cursed...
		// let file_path = dirs::home_dir()
		// 	.and_then(|mut path| path.join("algos.log").to_str().map(|s| s.to_owned()))
		// 	.unwrap_or_else(|| "algos.log".to_owned());

		// let file_rotator = file_rotate::FileRotate::new(
		// 	file_path,
		// 	file_rotate::suffix::AppendCount::new(5),
		// 	file_rotate::ContentLimit::BytesSurpassed(8 * 1024 * 1024),
		// 	file_rotate::compression::Compression::OnRotate(4),
		// 	None,
		// );
		// let decorator = slog_term::PlainSyncDecorator::new(file_rotator);
		// let drain1 = slog_term::FullFormat::new(decorator).build().fuse();
		// let drain1 = slog_async::Async::new(drain1).build().fuse();

		// let decorator = slog_term::TermDecorator::new().build();
		// let drain2 = slog_term::FullFormat::new(decorator).build().fuse();
		// let drain2 = slog_async::Async::new(drain2).build().fuse();

		let js_writer = JsWriter::new(
			env,
			env.get_global()?
				.get_property::<_, JsObject>(env.create_string("console")?)?
				.get_property::<_, JsFunction>(env.create_string("log")?)?,
		)?;

		let decorator = slog_term::PlainSyncDecorator::new(js_writer);
		let drain3 = slog_term::FullFormat::new(decorator).build().fuse();
		let drain3 = slog_async::Async::new(drain3).build().fuse();

		// let drain = drain1.combine(drain2).combine(drain3).fuse();

		let root_logger = slog::Logger::root(drain3, o!("class" => "VoiceEngine"));
		info!(root_logger, "Initialized"; "pid" => std::process::id());

		Ok(Self { root_logger, options: None })
	}

	pub(crate) fn logger(&self) -> &slog::Logger {
		&self.root_logger
	}
}

pub struct SyncVoiceEngine(SyncMutex<VoiceEngine>);

#[allow(unused)]
#[napi_derive_ext::module_interface]
impl SyncVoiceEngine {
	pub(crate) fn new(inner: VoiceEngine) -> Self {
		Self(SyncMutex::new(inner))
	}

	pub(crate) fn lock(&self) -> SyncMutexGuard<VoiceEngine> {
		self.0.lock()
	}

	#[module_interface(napi)]
	pub(crate) fn initialize(&self, options: EngineOptionsInput) -> Result<()> {
		let mut this = self.lock();
		if this.options.is_some() {
			return Err(napi::Error::from_reason("Engine already initialized"));
		}

		info!(this.logger(), "Engine initialized");
		this.options = Some(options.into());
		Ok(())
	}

	#[module_interface(napi)]
	pub(crate) fn set_clip_buffer_length(&self, length: f64) {
		info!(self.lock().logger(), "setClipBufferLength called (UNIMPLEMENTED)"; "length" => length);
	}

	#[module_interface(napi(js_name = "setEmitVADLevel"))]
	pub(crate) fn set_emit_vad_level(&self, emit: bool) {
		info!(self.lock().logger(), "setEmitVADLevel called (UNIMPLEMENTED)"; "emit" => emit);
	}

	#[module_interface(napi(js_name = "setEmitVADLevel2"))]
	pub(crate) fn set_emit_vad_level2(&self, emit: bool) {
		info!(self.lock().logger(), "setEmitVADLevel2 called (UNIMPLEMENTED)"; "emit" => emit);
	}

	#[module_interface(napi)]
	pub(crate) fn set_on_voice_callback(&self, callback: JsFunction) {
		info!(self.lock().logger(), "setOnVoiceCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_loopback(&self, loopback: bool, parameters: JsObject) {
		info!(self.lock().logger(), "setLoopback called (UNIMPLEMENTED)"; "loopback" => loopback);
	}

	#[module_interface(napi)]
	pub(crate) fn set_transport_options(&self, options: JsObject) {
		// TODO
		info!(self.lock().logger(), "setTransportOptions called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_device_change_callback(&self, callback: JsFunction) {
		// TODO
		info!(self.lock().logger(), "setDeviceChangeCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_output_device(&self, device_id: String) {
		// TODO
		info!(self.lock().logger(), "setOutputDevice called (UNIMPLEMENTED)"; "device_id" => device_id);
	}

	#[module_interface(napi)]
	pub(crate) fn set_input_device(&self, device_id: String) {
		// TODO
		info!(self.lock().logger(), "setInputDevice called (UNIMPLEMENTED)"; "device_id" => device_id);
	}

	#[module_interface(napi)]
	pub(crate) fn set_video_input_device(&self, device_id: String) {
		info!(self.lock().logger(), "setVideoInputDevice called (UNIMPLEMENTED)"; "device_id" => device_id);
	}

	#[module_interface(napi)]
	pub(crate) fn get_output_devices(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		// TODO
		info!(self.lock().logger(), "getOutputDevices called (IMPLEMENTED)");
		info!(self.lock().logger(), "Supported hosts:\n  {:?}", cpal::ALL_HOSTS);
		let available_hosts = cpal::available_hosts();
		info!(self.lock().logger(), "Available hosts:\n  {:?}", available_hosts);

		let mut output_devices = Vec::new();

		for host_id in available_hosts {
			let host = cpal::host_from_id(host_id).unwrap();
			info!(self.lock().logger(), "Host: {:?}", host.id());
			let devices = host.devices().unwrap();
			for device in devices {
				output_devices.push(DeviceInfo {
					name: format!("{} - {}", host_id.name(), device.name().unwrap()),
					guid: "".to_string(),
					index: output_devices.len() as u32,
				});
				info!(self.lock().logger(), "  Found Device: {:?}", device.name().unwrap());
			}
		}

		callback
			.call(
				None,
				&[napi::bindgen_prelude::Array::from_vec(&env, output_devices)?
					.coerce_to_object()?],
			)
			.unwrap();

		Ok(())
	}

	#[module_interface(napi)]
	pub(crate) fn get_input_devices(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		// TODO
		info!(self.lock().logger(), "getInputDevices called (IMPLEMENTED)");
		info!(self.lock().logger(), "Supported hosts:\n  {:?}", cpal::ALL_HOSTS);
		let available_hosts = cpal::available_hosts();
		info!(self.lock().logger(), "Available hosts:\n  {:?}", available_hosts);

		let mut input_devices = Vec::new();

		for host_id in available_hosts {
			let host = cpal::host_from_id(host_id).unwrap();
			info!(self.lock().logger(), "Host: {:?}", host.id());
			let devices = host.devices().unwrap();
			for device in devices {
				input_devices.push(DeviceInfo {
					name: format!("{} - {}", host_id.name(), device.name().unwrap()),
					guid: "".to_string(),
					index: input_devices.len() as u32,
				});
				info!(self.lock().logger(), "  Found Device: {:?}", device.name().unwrap());
			}
		}

		callback
			.call(
				None,
				&[napi::bindgen_prelude::Array::from_vec(&env, input_devices)?
					.coerce_to_object()?],
			)
			.unwrap();

		Ok(())
	}

	#[module_interface(napi)]
	pub(crate) fn get_video_input_devices(&self, callback: JsFunction) {
		info!(self.lock().logger(), "getVideoInputDevices called (HARD-CODED)");
		let tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, |ctx| {
				ctx.env.create_array_with_length(0).map(|x| vec![x])
			})
			.unwrap();
		tokio::spawn(async move {
			let _ = tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking);
		});
	}

	#[module_interface(napi)]
	pub(crate) fn set_video_output_sink(&self, callback: JsFunction) {
		info!(self.lock().logger(), "setVideoOutputSink called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn add_direct_video_output_sink(&self, stream_id: String) {
		info!(self.lock().logger(), "addDirectVideoOutputSink called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn remove_direct_video_output_sink(&self, stream_id: String) {
		info!(self.lock().logger(), "removeDirectVideoOutputSink called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn signal_video_output_sink_ready(&self, callback: JsFunction) {
		info!(self.lock().logger(), "signalVideoOutputSinkReady called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_image_data_allocator(&self, allocator: JsFunction) {
		info!(self.lock().logger(), "setImageDataAllocator called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_input_volume(&self, volume: f64) {
		info!(self.lock().logger(), "setInputVolume called (UNIMPLEMENTED)"; "volume" => volume);
	}

	#[module_interface(napi)]
	pub(crate) fn set_output_volume(&self, volume: f64) {
		info!(self.lock().logger(), "setOutputVolume called (UNIMPLEMENTED)"; "volume" => volume);
	}

	#[module_interface(napi)]
	pub(crate) fn set_volume_change_callback(&self, callback: JsFunction) {
		info!(self.lock().logger(), "setVolumeChangeCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_no_input_threshold(&self, what: JsFunction) {
		info!(self.lock().logger(), "setNoInputThreshold called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_no_input_callback(&self, callback: JsFunction) {
		info!(self.lock().logger(), "setNoInputCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn get_supported_video_codecs(&self) -> Result<Vec<String>> {
		info!(self.lock().logger(), "getSupportedVideoCodecs called (UNIMPLEMENTED)");
		Ok(vec![])
	}

	#[module_interface(napi)]
	pub(crate) fn get_codec_capabilities(
		&self,
		env: Env,
		callback: JsFunction,
	) -> napi::Result<()> {
		info!(self.lock().logger(), "getCodecCapabilities called (HARD-CODED)");
		callback.call(None, &[env.create_string("[{\"codec\":\"AV1X\",\"decode\":true,\"encode\":false},{\"codec\":\"H264\",\"decode\":true,\"encode\":true},{\"codec\":\"VP8\",\"decode\":true,\"encode\":true},{\"codec\":\"VP9\",\"decode\":true,\"encode\":true}]")?])?;

		Ok(())
	}

	#[module_interface(napi)]
	pub(crate) fn get_codec_survey(&self, what: JsFunction) {
		info!(self.lock().logger(), "getCodecSurvey called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_experimental_adm(&self, what: JsFunction) {
		info!(self.lock().logger(), "setExperimentalAdm called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn get_audio_subsystem(&self, callback: JsFunction) -> napi::Result<()> {
		info!(self.lock().logger(), "getAudioSubsystem called (HARD-CODED)");
		let tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, |ctx| {
				Ok(vec![
					ctx.env.create_string("standard")?,
					ctx.env.create_string("linuxPulseAudio")?,
				])
			})?;
		tokio::spawn(async move {
			let _ = tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking);
		});

		Ok(())
	}

	#[module_interface(napi)]
	pub(crate) fn get_desktop_sources(&self, what: JsFunction) {
		info!(self.lock().logger(), "getDesktopSources called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn ping_voice_thread(&self, what: JsFunction) {
		info!(self.lock().logger(), "pingVoiceThread called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn get_screen_previews(&self, what: JsFunction) {
		info!(self.lock().logger(), "getScreenPreviews called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn get_window_previews(&self, what: JsFunction) {
		info!(self.lock().logger(), "getWindowPreviews called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn console_log(&self, what: JsFunction) {
		info!(self.lock().logger(), "consoleLog called (UNPLANNED)");
	}

	#[module_interface(napi)]
	pub(crate) fn write_audio_debug_state(&self, what: JsFunction) {
		info!(self.lock().logger(), "writeAudioDebugState called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_aec_dump(&self, dump: bool) {
		info!(self.lock().logger(), "setAecDump called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn rank_rtc_regions(&self, what: JsFunction) {
		info!(self.lock().logger(), "rankRtcRegions called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn get_soundshare_status(&self, what: JsFunction) {
		info!(self.lock().logger(), "getSoundshareStatus called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn enable_soundshare(&self, what: JsFunction) {
		info!(self.lock().logger(), "enableSoundshare called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_video_input_initialization_callback(&self, what: JsFunction) {
		info!(self.lock().logger(), "setVideoInputInitializationCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn apply_media_filter_settings(&self, what: JsFunction) {
		info!(self.lock().logger(), "applyMediaFilterSettings called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn apply_media_filter_settings_with_callback(&self, what: JsFunction) {
		info!(self.lock().logger(), "applyMediaFilterSettingsWithCallback called (UNIMPLEMENTED)");
	}

	#[module_interface(napi)]
	pub(crate) fn set_max_sync_delay_override(&self, what: JsFunction) {
		info!(self.lock().logger(), "setMaxSyncDelayOverride called (UNIMPLEMENTED)");
	}

	// Custom methods to help with testing

	pub(crate) fn deinitialize(&self) {
		info!(self.lock().logger(), "deinitialize called");
		self.lock().root_logger = slog::Logger::root(slog::Discard, o!());
	}
}

impl Drop for SyncVoiceEngine {
	fn drop(&mut self) {
		info!(self.lock().logger(), "Engine dropped");
	}
}

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[repr(u32)]
#[napi]
pub enum DegradationPreference {
	MAINTAIN_RESOLUTION = 0,
	MAINTAIN_FRAMERATE = 1,
	BALANCED = 2,
	DISABLED = 3,
}

#[napi(object)]
pub(crate) struct DeviceInfo {
	pub name: String,
	pub guid: String,
	pub index: u32,
}

#[napi(object)]
pub(crate) struct EngineOptionsInput {
	pub audio_subsystem: String,
	pub data_directory: String,
	pub log_level: u32,
	pub use_fake_audio_capture: Option<bool>,
	pub use_fake_video_capture: Option<bool>,
	pub use_file_for_fake_audio_capture: Option<bool>,
	pub use_file_for_fake_video_capture: Option<bool>,
}

pub(crate) struct EngineOptions {
	pub audio_subsystem: String,
	pub data_directory: String,
	pub log_level: u32,
	pub use_fake_audio_capture: bool,
	pub use_fake_video_capture: bool,
	pub use_file_for_fake_audio_capture: bool,
	pub use_file_for_fake_video_capture: bool,
}

impl From<EngineOptionsInput> for EngineOptions {
	fn from(input: EngineOptionsInput) -> Self {
		Self {
			audio_subsystem: input.audio_subsystem,
			data_directory: input.data_directory,
			log_level: input.log_level,
			use_fake_audio_capture: input.use_fake_audio_capture.unwrap_or(false),
			use_fake_video_capture: input.use_fake_video_capture.unwrap_or(false),
			use_file_for_fake_audio_capture: input.use_file_for_fake_audio_capture.unwrap_or(false),
			use_file_for_fake_video_capture: input.use_file_for_fake_video_capture.unwrap_or(false),
		}
	}
}
