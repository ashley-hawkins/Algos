use std::{cmp, net::Ipv4Addr, random, time::Duration};

use discortp::rtp::{MutableRtpPacket, Rtp, RtpPacket};
use rtrb::chunks::ChunkError;
use slog::{info, warn};
use tokio::{
	select,
	sync::{oneshot, watch},
	time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::crypt::{self, VoiceConnectionCrypt};

use super::{
	connection_manager::{ConnectionManagerHandle, ConnectionManagerMessage},
	structures::IpDiscoveryPacket,
};

pub fn discover_ip<F: Fn(IpDiscoveryPacket) + Send + Sync + 'static>(
	logger: slog::Logger,
	ssrc: u32,
	connection_manager: ConnectionManagerHandle,
	callback: F,
) {
	tokio::spawn(async move {
		loop {
			let (sender, receiver) = oneshot::channel();
			if connection_manager
				.outbound
				.send_async(ConnectionManagerMessage::IpDiscovery {
					data: IpDiscoveryPacket::new_send_ssrc(ssrc),
					respond_to: sender,
				})
				.await
				.is_err()
			{
				warn!(logger, "Failed to send IP discovery message. Ending discovery.");
				break;
			}

			let res = timeout(Duration::from_secs(1), receiver).await;
			match res {
				Ok(Ok(packet)) => {
					tokio::time::sleep(Duration::from_millis(250)).await;
					info!(logger, "Discovered IP: {:#?}", packet.ip());
					callback(packet);
					break;
				}
				Ok(Err(_)) => {
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

type PingCallback = Box<dyn Fn(i64, u8) + Send + Sync>;
pub struct PingerHandle {
	pub ping_callback: watch::Sender<Option<PingCallback>>,
	pub ping_interval: watch::Sender<i64>,
}

pub fn start_pinger(
	logger: slog::Logger,
	_addr: (Ipv4Addr, u16),
	conn_manager: ConnectionManagerHandle,
	cancellation_token: CancellationToken,
) -> PingerHandle {
	let (ping_interval_tx, ping_interval_rx) = watch::channel(5000);
	let (ping_callback_tx, ping_callback_rx) = watch::channel::<Option<PingCallback>>(None);

	tokio::spawn(async move {
		let mut seq = 0u8;
		loop {
			let (sender, receiver) = oneshot::channel();
			if conn_manager
				.outbound
				.send_async(ConnectionManagerMessage::Ping { seq, respond_to: sender })
				.await
				.is_err()
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

					let callback = ping_callback_rx.borrow();
					if let Some(callback) = callback.as_ref() {
						callback(elapsed, seq);
					}
				}
				Ok(Err(_)) => {
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

pub fn start_voice_sender(
	logger: slog::Logger,
	ssrc: u32,
	mut reader: rtrb::Consumer<f32>,
	connection_manager: ConnectionManagerHandle,
	cancellation_token: CancellationToken,
) {
	const SAMPLES_PER_CHANNEL_PER_FRAME: usize = 960;
	const TOTAL_SAMPLES_PER_FRAME: usize = SAMPLES_PER_CHANNEL_PER_FRAME * 2;

	fn micros_from_samples(available: usize) -> u64 {
		(TOTAL_SAMPLES_PER_FRAME as u64
			- (available as u64).clamp(0, TOTAL_SAMPLES_PER_FRAME as u64))
			* 1_000_000
			/ 48_000 / 2
	}

	let mut encoder =
		opus::Encoder::new(48000, opus::Channels::Stereo, opus::Application::Audio).unwrap();

	tokio::spawn(async move {
		let mut buffer = [0.0; TOTAL_SAMPLES_PER_FRAME];
		let mut sequence = 0;
		let mut timestamp: u32 = random::random();
		loop {
			let available = match reader.read_chunk(TOTAL_SAMPLES_PER_FRAME) {
				Ok(read_chunk) => {
					for (src, dst) in read_chunk.into_iter().zip(&mut buffer) {
						*dst = src;
					}

					if let Ok(encoded) = encoder.encode_vec_float(&buffer, buffer.len()) {
						let rtp_packet = Rtp {
							version: 2,
							padding: false as u8,
							extension: false as u8,
							csrc_count: 0,
							marker: false as u8,
							payload_type: discortp::rtp::RtpType::Dynamic(120),
							sequence: sequence.into(),
							timestamp: timestamp.into(),
							ssrc,
							csrc_list: vec![],
							payload: encoded,
						};

						let packet_size = RtpPacket::packet_size(&rtp_packet);
						let mut buf = vec![
							0u8;
							packet_size
								+ crypt::constants::ENCRYPT_REQUIRED_EXTRA_CAPACITY
						];

						let mut packet = MutableRtpPacket::new(&mut buf[..packet_size]).unwrap();
						packet.populate(&rtp_packet);

						if connection_manager
							.outbound
							.try_send(ConnectionManagerMessage::Rtp { data: buf })
							.is_ok()
						{
							sequence = sequence.wrapping_add(1);
							timestamp =
								timestamp.wrapping_add(SAMPLES_PER_CHANNEL_PER_FRAME as u32);
						}
					}

					reader.slots()
				}
				Err(ChunkError::TooFewSlots(available)) => available,
			};

			select! {
				_ = tokio::time::sleep(Duration::from_micros(micros_from_samples(available))) => {}
				_ = cancellation_token.cancelled() => {
					break;
				}
			};
		}
	});
}
