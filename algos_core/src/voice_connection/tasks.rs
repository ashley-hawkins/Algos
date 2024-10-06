use std::{cmp, net::Ipv4Addr, time::Duration};

use slog::{info, warn};
use tokio::{
	select,
	sync::{oneshot, watch},
	time::timeout,
};
use tokio_util::sync::CancellationToken;

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
			if let Err(_) =
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
			if let Err(_) = conn_manager
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
