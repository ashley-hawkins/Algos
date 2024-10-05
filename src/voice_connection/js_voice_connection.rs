use cpal::{
	traits::{DeviceTrait, HostTrait, StreamTrait},
	Stream, SupportedBufferSize,
};
use ringbuf::traits::Split;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};

use crate::{
	crypt::{self, VoiceConnectionCrypt},
	engine::SyncVoiceEngine,
	voice_connection::{
		audio_thread::AudioThreadState,
		connection_manager::ConnectionManager,
		udp_connection,
		user_manager::{UserInitialData, UserManager, UserManagerMessage},
	},
	SyncMutex,
};

use std::{
	cell::RefCell,
	cmp,
	net::Ipv4Addr,
	str::FromStr,
	sync::{atomic::AtomicU32, Arc},
	time::Duration,
};

use napi::{
	bindgen_prelude::Array,
	threadsafe_function::{
		ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
	},
	Env, JsFunction, JsNumber, JsObject, JsUnknown,
};
use napi_derive::napi;
use slog::{info, o, warn};
use tokio::{
	sync::{oneshot, watch},
	time::timeout,
};

use super::{
	connection_manager::{ConnectionManagerHandle, ConnectionManagerMessage},
	structures::IpDiscoveryPacket,
	user_manager::UserManagerHandle,
};

type PingCallback = ThreadsafeFunction<(i64, u8), ErrorStrategy::Fatal>;
type IpDiscoveredCallback = ThreadsafeFunction<(Ipv4Addr, u16), ErrorStrategy::Fatal>;

fn discover_ip(
	logger: slog::Logger,
	ssrc: u32,
	connection_manager: ConnectionManagerHandle,
	callback: IpDiscoveredCallback,
) {
	tokio::spawn(async move {
		loop {
			let (sender, receiver) = oneshot::channel();
			if let Err(e) =
				connection_manager.outbound.send(ConnectionManagerMessage::IpDiscovery {
					data: IpDiscoveryPacket::new_send_ssrc(ssrc),
					respond_to: sender,
				}) {
				warn!(logger, "Failed to send IP discovery message: {e}");
				tokio::time::sleep(Duration::from_secs(5)).await;
				continue;
			}

			let res = timeout(Duration::from_millis(5000), receiver).await;
			match res {
				Ok(Ok(packet)) => {
					tokio::time::sleep(Duration::from_millis(500)).await;
					info!(logger, "Discovered IP: {:#?}", packet.ip());
					callback.call(
						(*packet.ip(), packet.port()),
						ThreadsafeFunctionCallMode::NonBlocking,
					);
					break;
				}
				_ => {
					warn!(logger, "Failed to discover IP, trying again in 5 seconds.");
					tokio::time::sleep(Duration::from_secs(5)).await;
				}
			}
		}
	});
}

struct PingerHandle {
	ping_callback: watch::Sender<Option<PingCallback>>,
	ping_interval: watch::Sender<i64>,
}

fn start_pinger(
	logger: slog::Logger,
	addr: (Ipv4Addr, u16),
	conn_manager: ConnectionManagerHandle,
) -> PingerHandle {
	let (ping_interval_tx, ping_interval_rx) = watch::channel(5000);
	let (ping_callback_tx, ping_callback_rx) = watch::channel::<Option<PingCallback>>(None);

	tokio::spawn(async move {
		let mut seq = 0u8;
		loop {
			let (sender, receiver) = oneshot::channel();
			if let Err(e) = conn_manager
				.outbound
				.send(ConnectionManagerMessage::Ping { seq, respond_to: sender })
			{
				warn!(logger, "Failed to send ping message, pinger will now exit.");
				break;
			}

			let interval = *ping_interval_rx.borrow();
			let current_time = std::time::Instant::now();
			let res = timeout(Duration::from_millis(interval as u64), receiver).await;
			let elapsed = current_time.elapsed().as_millis() as i64;

			match res {
				Ok(Ok(())) | Err(_) => {
					if res.is_err() {
						warn!(logger, "Ping response timed out");
					}
					let callback = ping_callback_rx.borrow().clone();
					if let Some(callback) = callback {
						let _ = callback.call((elapsed, 0), ThreadsafeFunctionCallMode::Blocking);
					}
				}
				Ok(Err(_)) => {
					warn!(logger, "Ping response was dropped, which means a ping was sent from somewhere else. This should never happen.");
				}
			}

			let remaining_wait = cmp::max(interval - elapsed, 0);
			tokio::time::sleep(Duration::from_millis(remaining_wait as u64)).await;

			seq = seq.wrapping_add(1);
		}
	});

	PingerHandle { ping_callback: ping_callback_tx, ping_interval: ping_interval_tx }
}

#[napi]
pub struct VoiceConnection {
	logger: slog::Logger,
	user_id: String,
	options: VoiceConnectionOptions,
	pinger: PingerHandle,
	user_manager: UserManagerHandle,
	crypt: Arc<SyncMutex<VoiceConnectionCrypt>>,
	stream: Stream,
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
		let engine = SyncVoiceEngine::instance(env);
		let logger = engine.lock().logger().new(o!("class" => "VoiceConnection"));
		let addr = (
			Ipv4Addr::from_str(&options.address)
				.map_err(|_| napi::Error::from_reason("Invalid IP address"))?,
			options.port,
		);

		info!(logger, "Connecting to {}:{}", addr.0, addr.1);

		let crypt = Arc::new(SyncMutex::new(VoiceConnectionCrypt::new()));

		let (audio_thread, audio_callback) = AudioThreadState::create_callback();

		let user_manager = UserManager::new(audio_thread).start();

		let conn = udp_connection::create_connection(logger.clone(), addr);

		let conn_manager =
			ConnectionManager::new(logger.new(o!("task" => "conn_manager")), crypt.clone())
				.start(conn, user_manager.clone());

		discover_ip(
			logger.new(o!("task" => "ip_discovery")),
			options.ssrc,
			conn_manager.clone(),
			callback.create_threadsafe_function(
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
			)?,
		);
		let pinger = start_pinger(logger.new(o!("task" => "pinger")), addr, conn_manager.clone());

		let dev = cpal::default_host().default_output_device().unwrap();
		let supported_config = dev.default_output_config().unwrap();
		let mut config = supported_config.config();
		config.channels = 2;
		config.sample_rate = cpal::SampleRate(48000);

		// if let SupportedBufferSize::Range { min, max } = supported_config.buffer_size() {
		// 	let mut max = *max;
		// 	if (max == u32::MAX) {
		// 		max = 48000 / 100;
		// 	}

		// 	config.buffer_size = cpal::BufferSize::Fixed(max);
		// }

		let stream = dev
			.build_output_stream(
				&config,
				audio_callback,
				move |err| {
					eprintln!("an error occurred on stream: {}", err);
				},
				None,
			)
			.unwrap();
		stream.play().unwrap();

		Ok(Self { logger, user_id, options, pinger, user_manager, crypt, stream })
	}

	#[napi]
	pub fn clear_desktop_source(&self) {
		info!(self.logger, "clearDesktopSource called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn configure_connection_retries(&self) {
		info!(self.logger, "configureConnectionRetries called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn destroy(&self) {
		info!(self.logger, "destroy called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn destroy_user(&self, user_id: String) {
		info!(self.logger, "destroyUser called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn get_encryption_modes(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "getEncryptionModes called (HARD-CODED)");

		let val = Array::from_ref_vec_string(
			&env,
			&[
				"aead_aes256_gcm_rtpsize".to_string(),
				"aead_aes256_gcm".to_string(),
				"aead_xchacha20_poly1305_rtpsize".to_string(),
				"xsalsa20_poly1305_lite_rtpsize".to_string(),
				"xsalsa20_poly1305_lite".to_string(),
				"xsalsa20_poly1305_suffix".to_string(),
				"xsalsa20_poly1305".to_string(),
			],
		)?;

		callback.call(None, &[val.coerce_to_object()?])?;

		Ok(())
	}

	#[napi]
	pub fn get_filtered_stats(
		&self,
		env: Env,
		filter: f64,
		callback: JsFunction,
	) -> napi::Result<()> {
		info!(self.logger, "getFilteredStats called (PARTIALLY IMPLEMENTED)");

		let filtered_stats = format!(
			r#"{{"clips":{{"clipDurationMs":0,"totalSavedKB":0}},"outbound":{{"audio":{{"audioLevel":0,"bytesSent":100,"codecName":"opus","codecPayloadType":120,"delayMedian":-1,"delayStd":-1,"echoReturnLoss":-1,"echoReturnLossEnhancement":-1,"encryptAttempts":0,"encryptDuration":0,"encryptFailureCount":0,"encryptMaxAttempts":0,"encryptSuccessCount":0,"fractionLost":-1,"framesCaptured":-1,"framesRendered":-1,"jitter":-1,"packetsLost":-1,"packetsSent":-1,"passthroughCount":-1,"residualEchoLikelihood":-1,"residualEchoLikelihoodRecentMax":-1,"rtt":-1,"speaking":0,"ssrc":{1},"typingNoiseDetected":false}},"id":"{0}"}},"transport":{{"bytesReceived":100,"bytesSent":1,"decryptionFailures":0,"inboundBitrateEstimate":0,"localAddress":"{2}","maxPaddingBitrate":0,"outboundBitrateEstimate":600000,"pacerDelay":0,"packetsReceived":0,"packetsSent":1,"receiverBitrateEstimate":0,"receiverReports":[],"routingFailures":0,"rtt":13,"sendBandwidth":600000}}}}"#,
			self.user_id, self.options.ssrc, ""
		);

		callback.call(None, &[env.create_string(&filtered_stats)?])?;

		Ok(())
	}

	#[napi]
	pub fn merge_users(&self, env: Env, users: JsObject) -> napi::Result<()> {
		info!(self.logger, "mergeUsers called (UNIMPLEMENTED)");

		let users: Vec<UserInitialData> = env.from_js_value(&users)?;

		info!(self.logger, "users: {users:#?}");

		self.user_manager.message_sender().send(UserManagerMessage::MergeUsers(users)).map_err(
			|e| napi::Error::from_reason(format!("Encountered an error while merging users: {e}")),
		)?;

		Ok(())
	}

	#[napi]
	pub fn set_desktop_source(&self) {
		info!(self.logger, "setDesktopSource called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_clip_record_ssrc(&self) {
		info!(self.logger, "setClipRecordSsrc called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_desktop_source_status_callback(&self, callback: JsFunction) {
		info!(self.logger, "setDesktopSourceStatusCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_desktop_source_with_options(&self) {
		info!(self.logger, "setDesktopSourceWithOptions called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_disable_local_video(&self) {
		info!(self.logger, "setDisableLocalVideo called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_local_mute(&self) {
		info!(self.logger, "setLocalMute called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_local_pan(&self) {
		info!(self.logger, "setLocalPan called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_local_volume(&self) {
		info!(self.logger, "setLocalVolume called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_minimum_output_delay(&self) {
		info!(self.logger, "setMinimumOutputDelay called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_no_input_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setNoInputCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_no_input_threshold(&self, _threshold: f64) {
		info!(self.logger, "setNoInputThreshold called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_first_frame_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnFirstFrameCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_desktop_source_ended(&self, _callback: JsFunction) {
		info!(self.logger, "setOnDesktopSourceEnded called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_soundshare(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshare called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_soundshare_ended(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshareEnded called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_soundshare_failed(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSoundshareFailed called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_speaking_callback(&self, env: Env, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setOnSpeakingCallback called (PARTIALLY IMPLEMENTED)");

		env.get_global()?
			.get_named_property::<JsObject>("console")?
			.get_named_property::<JsFunction>("log")?
			.call(None, &[&callback])?;

		let uid = self.user_id.clone();

		let tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, move |ctx| {
				Ok(vec![
					ctx.env.create_string(&uid)?.into_unknown(),
					ctx.env.create_uint32(0)?.into_unknown(),
				])
			})?;

		tokio::spawn(async move { tsfn.call((), ThreadsafeFunctionCallMode::Blocking) });

		Ok(())
	}

	#[napi]
	pub fn set_on_speaking_while_muted_callback(&self, callback: JsFunction) {
		info!(self.logger, "setOnSpeakingWhileMutedCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_video_callback(&self, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setOnVideoCallback called (PARTIALLY IMPLEMENTED)");

		let uid = self.user_id.clone();

		let logger = self.logger.clone();
		let tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal> = callback
			.create_threadsafe_function(0, move |ctx| {
				info!(logger, "WHAT!?!?!?!?!");
				Ok(vec![
					ctx.env.create_string(&uid)?.into_unknown(),
					ctx.env.create_uint32(0)?.into_unknown(),
					ctx.env.create_string("")?.into_unknown(),
					Array::from_ref_vec(&ctx.env, &([] as [JsNumber; 0]))?
						.coerce_to_object()?
						.into_unknown(),
				])
			})?;

		tokio::spawn(async move { tsfn.call((), ThreadsafeFunctionCallMode::Blocking) });

		Ok(())
	}

	#[napi(js_name = "setPTTActive")]
	pub fn set_ptt_active(&self) {
		info!(self.logger, "setPTTActive called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_on_video_encoder_fallback_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnVideoEncoderFallbackCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_secure_frames_state_update_callback(&self) {
		// info!(self.logger, "setSecureFramesStateUpdateCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_ping_callback(&mut self, callback: JsFunction) -> napi::Result<()> {
		info!(self.logger, "setPingCallback called (UNIMPLEMENTED)");
		self.pinger
			.ping_callback
			.send(Some(callback.create_threadsafe_function(
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
			)?))
			.map_err(|e| {
				napi::Error::from_reason(format!(
					"Encountered an error while setting ping callback: {e}"
				))
			})?;

		Ok(())
	}

	#[napi]
	pub fn set_ping_interval(&self, interval: f64) -> napi::Result<()> {
		info!(self.logger, "setPingInterval called (INTERVAL: {interval})");
		self.pinger.ping_interval.send(interval as i64).map_err(|e| {
			napi::Error::from_reason(format!(
				"Encountered an error while trying to set ping interval: {e}"
			))
		})?;

		Ok(())
	}

	#[napi]
	pub fn set_ping_timeout_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setPingTimeoutCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_remote_user_can_have_priority(&self) {
		info!(self.logger, "setRemoteUserCanHavePriority called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_remote_user_speaking_status(&self) {
		info!(self.logger, "setRemoteUserSpeakingStatus called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_rtc_log_marker(&self, _marker: String) {
		info!(self.logger, "setRtcLogMarker called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_self_deafen(&self, _deafen: bool) {
		info!(self.logger, "setSelfDeafen called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_self_mute(&self, _mute: bool) {
		info!(self.logger, "setSelfMute called (UNIMPLEMENTED)");
	}

	#[napi]
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
			crypt.set_mode(settings.mode);
			info!(self.logger, "Secret key: {:?}", settings.secret_key);
		} else {
			warn!(self.logger, "No encryption settings provided");
		}

		Ok(())
	}

	#[napi]
	pub fn set_video_broadcast(&self, _broadcast: bool) {
		info!(self.logger, "setVideoBroadcast called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn set_encryption(&self, _encryption: JsUnknown) {
		info!(self.logger, "setEncryption called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn start_replay(&self) {
		info!(self.logger, "startReplay called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn start_samples_playback(&self) {
		info!(self.logger, "startSamplesPlayback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn start_samples_local_playback(&self) {
		info!(self.logger, "startSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn stop_samples_playback(&self) {
		info!(self.logger, "stopSamplesPlayback called (UNIMPLEMENTED)");
	}

	pub fn stop_all_samples_local_playback(&self) {
		info!(self.logger, "stopAllSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn stop_samples_local_playback(&self) {
		info!(self.logger, "stopSamplesLocalPlayback called (UNIMPLEMENTED)");
	}

	#[napi(js_name = "setOnMLSFailureCallback")]
	pub fn set_on_mls_failure_callback(&self) {
		info!(self.logger, "setOnMLSFailureCallback called (UNIMPLEMENTED)");
	}

	#[napi]
	pub fn prepare_secure_frames_transition(&self) {
		info!(self.logger, "prepareSecureFramesTransition called (UNIMPLEMENTED)");
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
	pub mode: crypt::Mode,
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
