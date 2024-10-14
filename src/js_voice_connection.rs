use algos_core::voice_connection::{audio_thread::AudioInState, PingerHandle};
use cpal::{
	traits::{DeviceTrait, HostTrait, StreamTrait},
	Stream,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use tokio_util::sync::CancellationToken;

use crate::{
	crypt::{self, VoiceConnectionCrypt},
	engine::SyncVoiceEngine,
	voice_connection::{
		audio_thread::AudioOutState,
		connection_manager::ConnectionManager,
		udp_connection,
		user_manager::{UserInitialData, UserManager, UserManagerMessage},
	},
	SyncMutex,
};

use std::{net::Ipv4Addr, str::FromStr, sync::Arc};

use napi::{
	bindgen_prelude::Array,
	threadsafe_function::{
		ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
	},
	Env, JsFunction, JsObject, JsUnknown,
};
use napi_derive::napi;
use slog::{info, o, warn};

use crate::voice_connection::user_manager::UserManagerHandle;

type PingCallback = ThreadsafeFunction<(i64, u8), ErrorStrategy::Fatal>;
type IpDiscoveredCallback = ThreadsafeFunction<(Ipv4Addr, u16), ErrorStrategy::Fatal>;

struct VoiceConnectionInner {
	logger: slog::Logger,
	user_id: u64,
	options: VoiceConnectionOptions,
	pinger: PingerHandle,
	user_manager: UserManagerHandle,
	crypt: Arc<SyncMutex<VoiceConnectionCrypt>>,
	out_stream: Stream,
	in_stream: Stream,
	cancellation_token: CancellationToken,
}

impl VoiceConnectionInner {
	pub fn new(
		env: Env,
		user_id: String,
		options: VoiceConnectionOptions,
		callback: JsFunction,
	) -> napi::Result<Self> {
		let engine = SyncVoiceEngine::instance(env);
		let logger = engine.lock().logger().new(o!("class" => "VoiceConnection"));
		let video_thread = engine.lock().video_thread().clone();

		let addr = (
			Ipv4Addr::from_str(&options.address)
				.map_err(|_| napi::Error::from_reason("Invalid IP address"))?,
			options.port,
		);

		let user_id = user_id.parse().map_err(|e| napi::Error::from_reason(format!("{e}")))?;
		let ssrc = options.ssrc;

		let cancellation_token = CancellationToken::new();

		info!(logger, "Connecting to {}:{}", addr.0, addr.1);

		let crypt = Arc::new(SyncMutex::new(VoiceConnectionCrypt::new()));

		let (audio_out_handle, out_callback) = AudioOutState::create_callback();
		let (reader, in_callback) = AudioInState::create_callback();

		let user_manager = UserManager::new(audio_out_handle, video_thread).start();

		let conn = udp_connection::create_connection(logger.clone(), addr);

		let conn_manager =
			ConnectionManager::new(logger.new(o!("task" => "conn_manager")), crypt.clone())
				.start(conn, user_manager.clone());

		algos_core::voice_connection::discover_ip(
			logger.new(o!("task" => "ip_discovery")),
			options.ssrc,
			conn_manager.clone(),
			{
				let tsfn: IpDiscoveredCallback = callback.create_threadsafe_function(
					0,
					|ctx: ThreadSafeCallContext<(Ipv4Addr, u16)>| {
						let (address, port) = ctx.value;
						let mut connection_info = ctx.env.create_object()?;
						connection_info.set_named_property(
							"address",
							ctx.env.create_string(&address.to_string())?,
						)?;
						connection_info
							.set_named_property("port", ctx.env.create_uint32(port as u32)?)?;
						connection_info
							.set_named_property("protocol", ctx.env.create_string("udp")?)?;
						Ok(vec![
							ctx.env.create_string("")?.into_unknown(),
							connection_info.into_unknown(),
						])
					},
				)?;
				move |packet| {
					tsfn.call(
						(*packet.ip(), packet.port()),
						ThreadsafeFunctionCallMode::NonBlocking,
					);
				}
			},
		);

		algos_core::voice_connection::start_voice_sender(
			logger.clone(),
			ssrc,
			reader,
			conn_manager.clone(),
			cancellation_token.clone(),
		);

		let pinger = algos_core::voice_connection::start_pinger(
			logger.new(o!("task" => "pinger")),
			addr,
			conn_manager.clone(),
			cancellation_token.clone(),
		);

		let dev = cpal::default_host().default_output_device().unwrap();
		let supported_out_config = dev.default_output_config().unwrap();
		let mut out_config = supported_out_config.config();
		out_config.channels = 2;
		out_config.sample_rate = cpal::SampleRate(48000);

		let supported_in_config = dev.default_input_config().unwrap();
		let mut in_config = supported_in_config.config();
		in_config.channels = 2;
		in_config.sample_rate = cpal::SampleRate(48000);

		// if let SupportedBufferSize::Range { min, max } = supported_config.buffer_size() {
		// 	let mut max = *max;
		// 	if (max == u32::MAX) {
		// 		max = 48000 / 100;
		// 	}

		// 	config.buffer_size = cpal::BufferSize::Fixed(max);
		// }

		let out_stream = dev
			.build_output_stream(
				&out_config,
				out_callback,
				move |err| {
					eprintln!("an error occurred on stream: {}", err);
				},
				None,
			)
			.unwrap();
		out_stream.play().unwrap();

		let in_stream = dev
			.build_input_stream(
				&in_config,
				in_callback,
				move |err| {
					eprintln!("an error occurred on stream: {}", err);
				},
				None,
			)
			.unwrap();
		in_stream.play().unwrap();

		Ok(Self {
			logger,
			user_id,
			options,
			pinger,
			user_manager,
			crypt,
			out_stream,
			in_stream,
			cancellation_token,
		})
	}

	pub fn clear_desktop_source(&self) {
		info!(self.logger, "clearDesktopSource called (UNIMPLEMENTED)");
	}

	pub fn configure_connection_retries(&self) {
		info!(self.logger, "configureConnectionRetries called (UNIMPLEMENTED)");
	}

	pub fn destroy(&self) {
		info!(self.logger, "destroy called (UNIMPLEMENTED)");
	}

	pub fn destroy_user(&self, user_id: String) -> napi::Result<()> {
		info!(self.logger, "destroyUser called (IMPLEMENTED)");

		self.user_manager
			.message_sender()
			.send(UserManagerMessage::DestroyUser(
				user_id.parse().map_err(|e| napi::Error::from_reason(format!("{e}")))?,
			))
			.map_err(|e| {
				napi::Error::from_reason(format!("Encountered an error while destroying user: {e}"))
			})
	}

	pub fn get_encryption_modes(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "getEncryptionModes called (HARD-CODED)");

		let val = Array::from_ref_vec_string(&env, &[VoiceConnectionCrypt::MODE.to_owned()])?;

		callback.call(None, &[val.coerce_to_object()?])?;

		Ok(())
	}

	pub fn get_filtered_stats(
		&self,
		env: Env,
		_filter: f64,
		callback: JsFunction,
	) -> napi::Result<()> {
		// info!(self.logger, "getFilteredStats called (PARTIALLY IMPLEMENTED)");

		let filtered_stats = format!(
			r#"{{"clips":{{"clipDurationMs":0,"totalSavedKB":0}},"outbound":{{"audio":{{"audioLevel":0,"bytesSent":100,"codecName":"opus","codecPayloadType":120,"delayMedian":-1,"delayStd":-1,"echoReturnLoss":-1,"echoReturnLossEnhancement":-1,"encryptAttempts":0,"encryptDuration":0,"encryptFailureCount":0,"encryptMaxAttempts":0,"encryptSuccessCount":0,"fractionLost":-1,"framesCaptured":-1,"framesRendered":-1,"jitter":-1,"packetsLost":-1,"packetsSent":-1,"passthroughCount":-1,"residualEchoLikelihood":-1,"residualEchoLikelihoodRecentMax":-1,"rtt":-1,"speaking":0,"ssrc":{1},"typingNoiseDetected":false}},"id":"{0}"}},"transport":{{"bytesReceived":100,"bytesSent":1,"decryptionFailures":0,"inboundBitrateEstimate":0,"localAddress":"{2}","maxPaddingBitrate":0,"outboundBitrateEstimate":600000,"pacerDelay":0,"packetsReceived":0,"packetsSent":1,"receiverBitrateEstimate":0,"receiverReports":[],"routingFailures":0,"rtt":13,"sendBandwidth":600000}}}}"#,
			self.user_id, self.options.ssrc, ""
		);

		callback.call(None, &[env.create_string(&filtered_stats)?])?;

		Ok(())
	}

	pub fn merge_users(&self, env: Env, users: JsObject) -> napi::Result<()> {
		info!(self.logger, "mergeUsers called (UNIMPLEMENTED)");

		let users: Vec<UserInitialData> = env.from_js_value(&users)?;

		info!(self.logger, "users: {users:#?}");

		self.user_manager.message_sender().send(UserManagerMessage::MergeUsers(users)).map_err(
			|e| napi::Error::from_reason(format!("Encountered an error while merging users: {e}")),
		)?;

		Ok(())
	}

	pub fn set_desktop_source(&self) {
		info!(self.logger, "setDesktopSource called (UNIMPLEMENTED)");
	}

	pub fn set_clip_record_ssrc(&self) {
		info!(self.logger, "setClipRecordSsrc called (UNIMPLEMENTED)");
	}

	pub fn set_desktop_source_status_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setDesktopSourceStatusCallback called (UNIMPLEMENTED)");
	}

	pub fn set_desktop_source_with_options(&self) {
		info!(self.logger, "setDesktopSourceWithOptions called (UNIMPLEMENTED)");
	}

	pub fn set_disable_local_video(&self) {
		info!(self.logger, "setDisableLocalVideo called (UNIMPLEMENTED)");
	}

	pub fn set_local_mute(&self) {
		info!(self.logger, "setLocalMute called (UNIMPLEMENTED)");
	}

	pub fn set_local_pan(&self) {
		info!(self.logger, "setLocalPan called (UNIMPLEMENTED)");
	}

	pub fn set_local_volume(&self, user_id: String, volume: f32) -> napi::Result<()> {
		// // Inverse the volume transformation done by the client
		// let volume = (10.0 * volume.log10() + 50.0) / 50.0;

		// // Apply our own transformation
		// let volume = volume.powi(3);

		info!(
			self.logger,
			"setLocalVolume called (IMPLEMENTED) for user {user_id} with volume {volume}"
		);

		self.user_manager
			.message_sender()
			.send(UserManagerMessage::SetVolume(
				user_id.parse().map_err(|e| {
					napi::Error::from_reason(format!("Failed to parse user ID: {e}"))
				})?,
				volume,
			))
			.map_err(|e| {
				napi::Error::from_reason(format!("Encountered an error while setting volume: {e}"))
			})?;

		Ok(())
	}

	pub fn set_minimum_output_delay(&self) {
		info!(self.logger, "setMinimumOutputDelay called (UNIMPLEMENTED)");
	}

	pub fn set_no_input_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setNoInputCallback called (UNIMPLEMENTED)");
	}

	pub fn set_no_input_threshold(&self, _threshold: f64) {
		info!(self.logger, "setNoInputThreshold called (UNIMPLEMENTED)");
	}

	pub fn set_on_first_frame_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnFirstFrameCallback called (UNIMPLEMENTED)");
	}

	pub fn set_on_desktop_source_ended(&self, _callback: JsFunction) {
		info!(self.logger, "setOnDesktopSourceEnded called (UNIMPLEMENTED)");
	}

	pub fn set_on_soundshare(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshare called (UNIMPLEMENTED)");
	}

	pub fn set_on_soundshare_ended(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshareEnded called (UNIMPLEMENTED)");
	}

	pub fn set_on_soundshare_failed(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshareFailed called (UNIMPLEMENTED)");
	}

	pub fn set_on_speaking_callback(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setOnSpeakingCallback called (PARTIALLY IMPLEMENTED)");

		let uid = self.user_id.to_string();

		let tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, move |ctx| {
				Ok(vec![
					ctx.env.create_string(&uid)?.into_unknown(),
					ctx.env.create_uint32(1)?.into_unknown(),
				])
			})?;

		tokio::spawn(async move { tsfn.call((), ThreadsafeFunctionCallMode::Blocking) });

		Ok(())
	}

	pub fn set_on_speaking_while_muted_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSpeakingWhileMutedCallback called (UNIMPLEMENTED)");
	}

	pub fn set_on_video_callback(&self, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setOnVideoCallback called (PARTIALLY IMPLEMENTED)");

		let uid = self.user_id;

		let tsfn: ThreadsafeFunction<(String, u32, String), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, move |ctx| {
				let (uid, ssrc, stream_id): (String, u32, String) = ctx.value;
				Ok(vec![
					ctx.env.create_string(uid.as_str())?.into_unknown(),
					ctx.env.create_uint32(ssrc)?.into_unknown(),
					ctx.env.create_string(stream_id.as_str())?.into_unknown(),
					Array::from_vec(&ctx.env, Vec::<JsUnknown>::new())?
						.coerce_to_object()?
						.into_unknown(),
				])
			})?;

		let f = move |uid: u64, ssrc: u32, stream_id: Option<u64>| {
			tsfn.call(
				(uid.to_string(), ssrc, stream_id.map_or("".to_owned(), |id| id.to_string())),
				ThreadsafeFunctionCallMode::NonBlocking,
			);
		};

		tokio::spawn({
			let f = f.clone();
			async move {
				f(uid, 0, None);
			}
		});

		self.user_manager.set_on_video_callback(Box::new(f));

		Ok(())
	}

	pub fn set_ptt_active(&self) {
		info!(self.logger, "setPTTActive called (UNIMPLEMENTED)");
	}

	pub fn set_on_video_encoder_fallback_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnVideoEncoderFallbackCallback called (UNIMPLEMENTED)");
	}

	pub fn set_secure_frames_state_update_callback(&self) {
		// info!(self.logger, "setSecureFramesStateUpdateCallback called (UNIMPLEMENTED)");
	}

	pub fn set_ping_callback(&mut self, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setPingCallback called (UNIMPLEMENTED)");
		self.pinger
			.ping_callback
			.send(Some({
				let tsfn: PingCallback = callback.create_threadsafe_function(
					0,
					|ctx: ThreadSafeCallContext<(i64, u8)>| {
						let (interval, seq) = ctx.value;
						Ok(vec![
							ctx.env.create_int64(interval)?.into_unknown(),
							ctx.env.create_string("")?.into_unknown(),
							ctx.env.create_uint32(0)?.into_unknown(),
							ctx.env.create_uint32(seq as u32)?.into_unknown(),
						])
					},
				)?;

				Box::new(move |interval, seq| {
					tsfn.call((interval, seq), ThreadsafeFunctionCallMode::NonBlocking);
				})
			}))
			.map_err(|e| {
				napi::Error::from_reason(format!(
					"Encountered an error while setting ping callback: {e}"
				))
			})?;

		Ok(())
	}

	pub fn set_ping_interval(&self, interval: f64) -> napi::Result<()> {
		info!(self.logger, "setPingInterval called (INTERVAL: {interval})");
		self.pinger.ping_interval.send(interval as i64).map_err(|e| {
			napi::Error::from_reason(format!(
				"Encountered an error while trying to set ping interval: {e}"
			))
		})?;

		Ok(())
	}

	pub fn set_ping_timeout_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setPingTimeoutCallback called (UNIMPLEMENTED)");
	}

	pub fn set_remote_user_can_have_priority(&self) {
		info!(self.logger, "setRemoteUserCanHavePriority called (UNIMPLEMENTED)");
	}

	pub fn set_remote_user_speaking_status(&self) {
		info!(self.logger, "setRemoteUserSpeakingStatus called (UNIMPLEMENTED)");
	}

	pub fn set_rtc_log_marker(&self, _marker: String) {
		info!(self.logger, "setRtcLogMarker called (UNIMPLEMENTED)");
	}

	pub fn set_self_deafen(&self, _deafen: bool) {
		info!(self.logger, "setSelfDeafen called (UNIMPLEMENTED)");
	}

	pub fn set_self_mute(&self, _mute: bool) {
		info!(self.logger, "setSelfMute called (UNIMPLEMENTED)");
	}

	pub fn set_transport_options(&self, env: Env, transport_options: JsObject) -> napi::Result<()> {
		info!(self.logger, "setTransportOptions called (PARTIALLY IMPLEMENTED)");

		let transport_options: TransportOptions = env.from_js_value(&transport_options)?;

		if let Some(settings) = transport_options.encryption_settings {
			info!(self.logger, "Encryption settings provided");
			let mut crypt = self.crypt.lock();
			if (settings.secret_key.len() != crypt::constants::KEY_BYTES) {
				warn!(self.logger, "Invalid key length provided");
				return Err(napi::Error::from_reason("Invalid key length"));
			}

			crypt.set_key(settings.secret_key.as_slice().try_into().unwrap());
			info!(self.logger, "Secret key set to {:?}", settings.secret_key);

			if settings.mode != VoiceConnectionCrypt::MODE {
				warn!(self.logger, "Invalid mode provided");
				return Err(napi::Error::from_reason("Invalid mode"));
			}

			// crypt.set_mode(settings.mode);
			info!(self.logger, "Secret key: {:?}", settings.secret_key);
		} else {
			warn!(self.logger, "No encryption settings provided");
		}

		Ok(())
	}

	pub fn set_video_broadcast(&self, _broadcast: bool) {
		info!(self.logger, "setVideoBroadcast called (UNIMPLEMENTED)");
	}

	pub fn set_encryption(&self, _encryption: JsUnknown) {
		info!(self.logger, "setEncryption called (UNIMPLEMENTED)");
	}

	pub fn start_replay(&self) {
		info!(self.logger, "startReplay called (UNIMPLEMENTED)");
	}

	pub fn start_samples_playback(&self) {
		info!(self.logger, "startSamplesPlayback called (UNIMPLEMENTED)");
	}

	pub fn start_samples_local_playback(&self) {
		info!(self.logger, "startSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	pub fn stop_samples_playback(&self) {
		info!(self.logger, "stopSamplesPlayback called (UNIMPLEMENTED)");
	}

	pub fn stop_all_samples_local_playback(&self) {
		info!(self.logger, "stopAllSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	pub fn stop_samples_local_playback(&self) {
		info!(self.logger, "stopSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	pub fn set_on_mls_failure_callback(&self) {
		info!(self.logger, "setOnMLSFailureCallback called (UNIMPLEMENTED)");
	}

	pub fn prepare_secure_frames_transition(&self) {
		info!(self.logger, "prepareSecureFramesTransition called (UNIMPLEMENTED)");
	}
}

impl Drop for VoiceConnectionInner {
	fn drop(&mut self) {
		self.cancellation_token.cancel();
	}
}

#[napi]
pub struct VoiceConnection {
	inner: Option<VoiceConnectionInner>,
}

#[napi]
impl VoiceConnection {
	#[napi(constructor)]
	pub fn new(
		env: Env,
		user_id: String,
		options: VoiceConnectionOptions,
		callback: JsFunction,
	) -> napi::Result<Self> {
		Ok(Self { inner: Some(VoiceConnectionInner::new(env, user_id, options, callback)?) })
	}

	#[napi]
	pub fn clear_desktop_source(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.clear_desktop_source();
		}
	}

	#[napi]
	pub fn configure_connection_retries(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.configure_connection_retries();
		}
	}

	#[napi]
	pub fn destroy(&mut self) {
		self.inner = None;
	}

	#[napi]
	pub fn destroy_user(&self, user_id: String) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.destroy_user(user_id)?;
		}

		Ok(())
	}

	#[napi]
	pub fn get_encryption_modes(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.get_encryption_modes(env, callback)?;
		}
		Ok(())
	}

	#[napi]
	pub fn get_filtered_stats(
		&self,
		env: Env,
		filter: f64,
		callback: JsFunction,
	) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.get_filtered_stats(env, filter, callback)?;
		}
		Ok(())
	}

	#[napi]
	pub fn merge_users(&self, env: Env, users: JsObject) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.merge_users(env, users)?;
		}

		Ok(())
	}

	#[napi]
	pub fn set_desktop_source(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_desktop_source();
		}
	}

	#[napi]
	pub fn set_clip_record_ssrc(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_clip_record_ssrc();
		}
	}

	#[napi]
	pub fn set_desktop_source_status_callback(&self, callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_desktop_source_status_callback(callback);
		}
	}

	#[napi]
	pub fn set_desktop_source_with_options(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_desktop_source_with_options();
		}
	}

	#[napi]
	pub fn set_disable_local_video(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_disable_local_video();
		}
	}

	#[napi]
	pub fn set_local_mute(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_local_mute();
		}
	}

	#[napi]
	pub fn set_local_pan(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_local_pan();
		}
	}

	#[napi]
	pub fn set_local_volume(&self, user_id: String, volume: f64) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_local_volume(user_id, volume as f32)?;
		}

		Ok(())
	}

	#[napi]
	pub fn set_minimum_output_delay(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_minimum_output_delay();
		}
	}

	#[napi]
	pub fn set_no_input_callback(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_no_input_callback(_callback);
		}
	}

	#[napi]
	pub fn set_no_input_threshold(&self, _threshold: f64) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_no_input_threshold(_threshold);
		}
	}

	#[napi]
	pub fn set_on_first_frame_callback(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_first_frame_callback(_callback);
		}
	}

	#[napi]
	pub fn set_on_desktop_source_ended(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_desktop_source_ended(_callback);
		}
	}

	#[napi]
	pub fn set_on_soundshare(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_soundshare(_callback);
		}
	}

	#[napi]
	pub fn set_on_soundshare_ended(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_soundshare_ended(_callback);
		}
	}

	#[napi]
	pub fn set_on_soundshare_failed(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_soundshare_failed(_callback);
		}
	}

	#[napi]
	pub fn set_on_speaking_callback(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_speaking_callback(env, callback)?;
		}
		Ok(())
	}

	#[napi]
	pub fn set_on_speaking_while_muted_callback(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_speaking_while_muted_callback(_callback);
		}
	}

	#[napi]
	pub fn set_on_video_callback(&self, callback: JsFunction) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_video_callback(callback)?;
		}
		Ok(())
	}

	#[napi(js_name = "setPTTActive")]
	pub fn set_ptt_active(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_ptt_active();
		}
	}

	#[napi]
	pub fn set_on_video_encoder_fallback_callback(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_video_encoder_fallback_callback(_callback);
		}
	}

	#[napi]
	pub fn set_secure_frames_state_update_callback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_secure_frames_state_update_callback();
		}
	}

	#[napi]
	pub fn set_ping_callback(&mut self, callback: JsFunction) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_mut() {
			inner.set_ping_callback(callback)?;
		}
		Ok(())
	}

	#[napi]
	pub fn set_ping_interval(&self, interval: f64) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_ping_interval(interval)?;
		}
		Ok(())
	}

	#[napi]
	pub fn set_ping_timeout_callback(&self, _callback: JsFunction) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_ping_timeout_callback(_callback);
		}
	}

	#[napi]
	pub fn set_remote_user_can_have_priority(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_remote_user_can_have_priority();
		}
	}

	#[napi]
	pub fn set_remote_user_speaking_status(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_remote_user_speaking_status();
		}
	}

	#[napi]
	pub fn set_rtc_log_marker(&self, _marker: String) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_rtc_log_marker(_marker);
		}
	}

	#[napi]
	pub fn set_self_deafen(&self, _deafen: bool) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_self_deafen(_deafen);
		}
	}

	#[napi]
	pub fn set_self_mute(&self, _mute: bool) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_self_mute(_mute);
		}
	}

	#[napi]
	pub fn set_transport_options(&self, env: Env, transport_options: JsObject) -> napi::Result<()> {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_transport_options(env, transport_options)?;
		}

		Ok(())
	}

	#[napi]
	pub fn set_video_broadcast(&self, _broadcast: bool) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_video_broadcast(_broadcast);
		}
	}

	#[napi]
	pub fn set_encryption(&self, _encryption: JsUnknown) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_encryption(_encryption);
		}
	}

	#[napi]
	pub fn start_replay(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.start_replay();
		}
	}

	#[napi]
	pub fn start_samples_playback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.start_samples_playback();
		}
	}

	#[napi]
	pub fn start_samples_local_playback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.start_samples_local_playback();
		}
	}

	#[napi]
	pub fn stop_samples_playback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.stop_samples_playback();
		}
	}

	#[napi]
	pub fn stop_all_samples_local_playback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.stop_all_samples_local_playback();
		}
	}

	#[napi]
	pub fn stop_samples_local_playback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.stop_samples_local_playback();
		}
	}

	#[napi(js_name = "setOnMLSFailureCallback")]
	pub fn set_on_mls_failure_callback(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_on_mls_failure_callback();
		}
	}

	#[napi]
	pub fn prepare_secure_frames_transition(&self) {
		if let Some(inner) = self.inner.as_ref() {
			inner.prepare_secure_frames_transition();
		}
	}
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InputModeOptions {
	vad_auto_threshold: f64,
	vad_leading: f64,
	vad_threshold: f64,
	vad_trailing: f64,
	vad_use_krisp: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AltTransportOptions {
	input_mode: f64,
	input_mode_options: Option<InputModeOptions>,
	remote_sink_wants_max_framerate: f64,
	attenuate_while_speaking_others: bool,
	attenuate_while_speaking_self: bool,
	attenuation: bool,
	attenuation_factor: bool,
	qos: bool,
	experimental_encoders: bool,
	hardware_h264: bool,
	encoding_video_bit_rate: f64,
	encoding_video_frame_rate: f64,
	encoding_video_height: f64,
	encoding_video_max_bit_rate: f64,
	encoding_video_min_bit_rate: f64,
	encoding_video_width: f64,
	remote_sink_wants_pixel_count: f64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EncryptionTransportOptions {
	#[serde_as(as = "TryFromInto<String>")]
	pub mode: String,
	pub secret_key: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TransportOptions {
	#[serde(flatten)]
	pub alt_transport_options: Option<AltTransportOptions>,
	pub encryption_settings: Option<EncryptionTransportOptions>,
}

#[napi(object)]
pub struct Resolution {
	pub _type: String,
	pub width: f64,
	pub height: f64,
}

#[napi(object)]
pub struct StreamParameters {
	pub active: Option<bool>,
	pub max_bitrate: Option<f64>,
	pub max_framerate: Option<f64>,
	pub max_resolution: Option<Resolution>,
	pub profile: Option<String>,
	pub quality: Option<f64>,
	pub rid: Option<f64>,
	pub rtx_ssrc: Option<f64>,
	pub ssrc: Option<f64>,
	pub _type: Option<String>,
}

#[napi(object)]
pub struct VoiceConnectionOptions {
	pub address: String,
	pub experiments: Vec<String>,
	pub modes: Vec<String>,
	pub port: u16,
	pub qos_enabled: bool,
	pub ssrc: u32,
	pub stream_parameters: Option<StreamParameters>,
}
