use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use discortp::{
	rtp::{RtpExtensionPacket, RtpPacket, RtpType},
	Packet,
};
use slog::{info, warn};
use tokio::{net::UdpSocket, select, time::timeout};

use crate::constants;

use super::structures::IpDiscoveryPacket;

#[derive(Debug)]
pub enum VoiceConnMessage {
	Ping { seq: u8 },
	IpDiscovery(IpDiscoveryPacket),
	Rtp(Vec<u8>),
}

impl From<VoiceConnMessage> for Vec<u8> {
	fn from(value: VoiceConnMessage) -> Self {
		match value {
			VoiceConnMessage::Ping { seq } => {
				let mut buf = Vec::with_capacity(8);
				buf.extend_from_slice(&0x1337CAFEu32.to_be_bytes());
				buf.push(seq);
				buf.resize(8, 0);
				buf
			}

			VoiceConnMessage::IpDiscovery(data) => data.into(),
			VoiceConnMessage::Rtp(data) => data,
		}
	}
}

impl TryFrom<&[u8]> for VoiceConnMessage {
	type Error = ();

	fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
		match value.len() {
			8 => Ok(VoiceConnMessage::Ping { seq: value[4] }),

			x if x == const { IpDiscoveryPacket::packet_size() } => {
				Ok(VoiceConnMessage::IpDiscovery(value.try_into()?))
			}

			12.. => Ok(VoiceConnMessage::Rtp({
				let owned = value.to_owned();
				let rtp_packet_view = RtpPacket::new(owned.as_slice()).unwrap();

				// = It's not RTP
				if matches!(rtp_packet_view.get_payload_type(), RtpType::Reserved(_)) {
					return Err(());
				}

				if rtp_packet_view.get_extension() == 1 {
					// = Packet is too short to contain an extension
					let extension = RtpExtensionPacket::new(rtp_packet_view.payload()).ok_or(())?;

					// = Extension length is invalid
					if extension.get_length() as usize * 4 > extension.payload().len() {
						return Err(());
					}
				}

				owned
			})),

			_ => Err(()),
		}
	}
}

pub struct ConnectionHandle {
	pub outbound: flume::Sender<VoiceConnMessage>,
	pub inbound: flume::Receiver<VoiceConnMessage>,
}

pub fn create_connection(logger: slog::Logger, addr: (Ipv4Addr, u16)) -> ConnectionHandle {
	const UDP_MAX_PACKET_SIZE: usize = u16::MAX as usize; // = 65535

	let (outbound_tx, outbound_rx) =
		flume::bounded::<VoiceConnMessage>(constants::MAIN_CHANNELS_SIZE);
	let (inbound_tx, inbound_rx) = flume::bounded(constants::MAIN_CHANNELS_SIZE);

	tokio::spawn(async move {
		let sock = Arc::new(
			match timeout(Duration::from_secs(10), UdpSocket::bind((Ipv4Addr::from(0), 0))).await {
				Ok(Ok(conn)) => conn,
				_ => {
					warn!(logger, "Failed to bind UDP socket");
					tokio::time::sleep(Duration::from_secs(10)).await;
					return;
				}
			},
		);

		match sock.connect(addr).await {
			Ok(_) => {}
			_ => {
				warn!(logger, "Failed to connect UDP socket");
				tokio::time::sleep(Duration::from_secs(10)).await;
				return;
			}
		}

		let mut recv_buf = [0; UDP_MAX_PACKET_SIZE];

		loop {
			// info!(logger, "Connection task loop iterating");
			select! {
				Ok(msg) = outbound_rx.recv_async() => {
					// info!(logger, "Sending {:?}", msg);
					let data: Vec<u8> = msg.into();
					match sock.send(&data).await {
						Ok(_) => {}
						Err(e) => {
							warn!(logger, "Failed to send message: {e}");
							continue;
						}
					};
				}
				res = sock.recv_from(&mut recv_buf) => {
					let (len, addr) = match res {
						Ok((len, addr)) => (len, addr),
						Err(e) => {
							warn!(logger, "Failed to receive message: {e}");
							continue;
						}
					};
					let data = &recv_buf[..len];
					let msg: Result<VoiceConnMessage, _> = data.try_into();
					match msg {
						Ok(msg) => {
							// info!(logger, "Received {:#?}", msg);
							if let Err(e) = inbound_tx.send_async(msg).await {
								warn!(logger, "Failed to send message to manager: {e}. Closing connection.");
								break;
							}
						}
						Err(_) => {
							info!(logger, "Received invalid message: {:?} from {}", data, addr);
						}
					}
				}
				else => {
					break;
				}
			}
		}

		warn!(logger, "Connection task ended");
	});

	ConnectionHandle { outbound: outbound_tx, inbound: inbound_rx }
}
