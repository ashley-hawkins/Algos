use cpal::{
	traits::{DeviceTrait, HostTrait, StreamTrait},
	Stream,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use strum::IntoEnumIterator;
use tokio_util::sync::CancellationToken;

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
	cmp,
	net::Ipv4Addr,
	str::FromStr,
	sync::Arc,
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
	select,
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
				warn!(logger, "Failed to send IP discovery message. Ending discovery.");
				break;
			}

			let res = timeout(Duration::from_secs(1), receiver).await;
			match res {
				Ok(Ok(packet)) => {
					tokio::time::sleep(Duration::from_millis(250)).await;
					info!(logger, "Discovered IP: {:#?}", packet.ip());
					callback.call(
						(*packet.ip(), packet.port()),
						ThreadsafeFunctionCallMode::NonBlocking,
					);
					break;
				}
				Ok(Err(e)) => {
					warn!(logger, "Failed to receive IP discovery response. Ending discovery.");
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
	cancellation_token: CancellationToken,
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

			let res = select! {
				res = timeout(Duration::from_millis(interval as u64), receiver) => {
					res
				}
				_ = cancellation_token.cancelled() => {
					break;
				}
			};

			let elapsed = current_time.elapsed().as_millis() as i64;

			match res {
				Ok(Ok(_)) | Err(_) => {
					if res.is_err() {
						warn!(logger, "Ping response timed out");
					}
					let callback = ping_callback_rx.borrow().clone();
					if let Some(callback) = callback {
						let _ = callback.call((elapsed, 0), ThreadsafeFunctionCallMode::Blocking);
					}
				}
				Ok(Err(e)) => {
					warn!(logger, "Ping response was dropped, either means a ping was sent from somewhere else or the voice connection was destroyed. The former should never happen.");
				}
			}

			let remaining_wait = cmp::max(interval - elapsed, 0);
			tokio::time::sleep(Duration::from_millis(remaining_wait as u64)).await;

			seq = seq.wrapping_add(1);
		}
	});

	PingerHandle { ping_callback: ping_callback_tx, ping_interval: ping_interval_tx }
}

struct VoiceConnectionInner {
	logger: slog::Logger,
	user_id: String,
	options: VoiceConnectionOptions,
	pinger: PingerHandle,
	user_manager: UserManagerHandle,
	crypt: Arc<SyncMutex<VoiceConnectionCrypt>>,
	stream: Stream,
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
		let addr = (
			Ipv4Addr::from_str(&options.address)
				.map_err(|_| napi::Error::from_reason("Invalid IP address"))?,
			options.port,
		);

		let cancellation_token = CancellationToken::new();

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

		let pinger = start_pinger(
			logger.new(o!("task" => "pinger")),
			addr,
			conn_manager.clone(),
			cancellation_token.clone(),
		);

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

		Ok(Self {
			logger,
			user_id,
			options,
			pinger,
			user_manager,
			crypt,
			stream,
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

		let val = Array::from_ref_vec_string(
			&env,
			&crypt::Mode::iter().map(|mode| mode.into()).collect::<Vec<String>>(),
		)?;

		callback.call(None, &[val.coerce_to_object()?])?;

		Ok(())
	}

	pub fn get_filtered_stats(
		&self,
		env: Env,
		filter: f64,
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

	pub fn set_desktop_source_status_callback(&self, callback: JsFunction) {
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

	pub fn set_local_volume(&self, user_id: String, volume: f64) {
		info!(
			self.logger,
			"setLocalVolume called (UNIMPLEMENTED) for user {user_id} with volume {volume}"
		);
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

	pub fn set_on_speaking_while_muted_callback(&self, _callback: JsFunction) {
		info!(self.logger, "setOnSpeakingWhileMutedCallback called (UNIMPLEMENTED)");
	}

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
			crypt.set_mode(settings.mode);
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
	pub fn set_local_volume(&self, user_id: String, volume: f64) {
		if let Some(inner) = self.inner.as_ref() {
			inner.set_local_volume(user_id, volume);
		}
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
