use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use cpal::traits::{DeviceTrait, HostTrait};
use slog::{info, warn};
use tokio::{net::UdpSocket, select, sync::oneshot, time::timeout};

use super::structures::{IpDiscoveryPacket, RtpPacket};

use crate::{connection::structures::RtpPacketTrait, crypt::VoiceConnectionCrypt, SyncMutex};

#[derive(Debug)]
enum VoiceConnMessage {
	Ping { seq: u8 },
	IpDiscovery(IpDiscoveryPacket),
	Rtp(RtpPacket),
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
			VoiceConnMessage::Rtp(data) => data.into_raw(),
		}
	}
}

impl TryFrom<&[u8]> for VoiceConnMessage {
	type Error = ();

	fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
		match value.len() {
			8 => Ok(VoiceConnMessage::Ping { seq: value[4] }),

			const { IpDiscoveryPacket::packet_size() } => {
				Ok(VoiceConnMessage::IpDiscovery(value.try_into()?))
			}

			12.. => Ok(VoiceConnMessage::Rtp(value.try_into()?)),

			_ => Err(()),
		}
	}
}

#[derive(Debug)]
pub enum ConnectionManagerMessage {
	Ping { seq: u8, respond_to: oneshot::Sender<()> },
	IpDiscovery { data: IpDiscoveryPacket, respond_to: oneshot::Sender<IpDiscoveryPacket> },
	Rtp { data: RtpPacket },
}

#[derive(Clone)]
pub struct ConnectionManagerHandle {
	pub outbound: tokio::sync::mpsc::UnboundedSender<ConnectionManagerMessage>,
}

pub(crate) fn create_connection_manager(
	logger: slog::Logger,
	crypt: Arc<SyncMutex<VoiceConnectionCrypt>>,
	mut connection: ConnectionHandle,
) -> ConnectionManagerHandle {
	let (outbound_tx, mut outbound_rx) =
		tokio::sync::mpsc::unbounded_channel::<ConnectionManagerMessage>();
	tokio::spawn(async move {
		let mut last_ping_respond_to: Option<(u8, oneshot::Sender<()>)> = None;
		let mut last_ip_discovery_respond_to: Option<oneshot::Sender<IpDiscoveryPacket>> = None;

		loop {
			select! {
				Some(msg) = connection.inbound.recv() => {
					// println!("Received from voice server: {:#?}", msg);
					match msg {
						VoiceConnMessage::Ping { seq } => {
							if let Some((want_seq, respond_to)) = last_ping_respond_to.take() {
								if want_seq == seq {
									if respond_to.send(()).is_err() {
										warn!(logger, "Failed to send ping response back to caller.");
									}
								} else {
									last_ping_respond_to = Some((want_seq, respond_to));
								}
							}
						}
						VoiceConnMessage::IpDiscovery(data) => {
							if let Some(respond_to) = last_ip_discovery_respond_to.take() {
								// TODO: Handle fail
								if respond_to.send(data).is_err() {
									warn!(logger, "Failed to send IP discovery response back to caller.");
								};
							}
						}
						VoiceConnMessage::Rtp(data) => {
							// TODO: Handle receiving RTP packets
							let ssrc = data.ssrc();
							let csrc_count = data.csrc_count();
							let ext = data.extension();
							let has_ext = ext.is_some();
							let ext_id = ext.as_ref().map(| value | value.id());
							let ext_len = ext.as_ref().map(| value | value.len());
							let ext_payload = ext.as_ref().map(| value | value.payload().to_owned());
							let payload_type = data.payload_type();
							let data_original = data.into_raw();
							let mut data = data_original.clone();
							let mut crypt = crypt.lock();
							if let Some((header_length, total_length)) = crypt.decrypt_in_place(&mut data) {
								info!(logger, "Received RTP packet. Header length: {header_length}, Total length: {total_length}, Ssrc: {ssrc}, Packet type: {payload_type}");
								if(payload_type == 120) {
									let mut decoder = opus::Decoder::new(48000, opus::Channels::Stereo).unwrap();
									let mut output = [0; 5760 * 2];
									match decoder.decode(&data[header_length..total_length], &mut output,false) {
											Ok(_) => { info!(logger, "Decoded opus packet: {ssrc} {csrc_count} {total_length} {header_length} {has_ext} {ext_id:?} {ext_len:?} {ext_payload:?} Data: {data:?} Data Original: {data_original:?}"); },
											Err(e) => { warn!(logger, "Failed to decode opus packet: {e}; {ssrc} {csrc_count} {total_length} {header_length} {has_ext} {ext_id:?} {ext_len:?} {ext_payload:?} Data: {data:?} Data Original: {data_original:?}"); },
									};
									info!(logger, "Decoded audio packet: {}", output.len());
								}
							} else {
								warn!(logger, "Failed to decrypt RTP packet. Ssrc: {ssrc}");
							}
						}}
				},
				Some(msg) = outbound_rx.recv() => {
					// println!("Received message to send out: {:#?}", msg);
					match msg {
						ConnectionManagerMessage::Ping { seq, respond_to } => {
							last_ping_respond_to = Some((seq, respond_to));
							if let Err(e) = connection.outbound.send(VoiceConnMessage::Ping { seq }) {
								warn!(logger, "Failed to send ping message: {e}");
							};
						}
						ConnectionManagerMessage::IpDiscovery { data, respond_to } => {
							last_ip_discovery_respond_to = Some(respond_to);
							if let Err(e) = connection.outbound.send(VoiceConnMessage::IpDiscovery(data)) {
								warn!(logger, "Failed to send IP discovery message: {e}");
							}
						}
						ConnectionManagerMessage::Rtp { data } => {
							if let Err(e) = connection.outbound.send(VoiceConnMessage::Rtp(data)) {
								warn!(logger, "Failed to send RTP message: {e}");
							}
						}
					}
				},
				else => {
					break;
				}
			}
		}
	});
	ConnectionManagerHandle { outbound: outbound_tx }
}

pub(crate) struct ConnectionHandle {
	outbound: tokio::sync::mpsc::UnboundedSender<VoiceConnMessage>,
	inbound: tokio::sync::mpsc::UnboundedReceiver<VoiceConnMessage>,
}

pub(crate) fn create_connection(logger: slog::Logger, addr: (Ipv4Addr, u16)) -> ConnectionHandle {
	const UDP_MAX_PACKET_SIZE: usize = u16::MAX as usize; // = 65535

	let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<VoiceConnMessage>();
	let (mut inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();

	tokio::spawn(async move {
		let mut sock = Arc::new(
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
			info!(logger, "Connection task loop iterating");
			select! {
				Some(msg) = outbound_rx.recv() => {
					info!(logger, "Sending {:#?}", msg);
					let data: Vec<u8> = msg.into();
					match sock.send(&data).await {
						Ok(_) => {}
						Err(e) => {
							warn!(logger, "Failed to send message: {e}");
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
							if let Err(e) = inbound_tx.send(msg) {
								warn!(logger, "Failed to send message to manager: {e}");
							}
						}
						Err(_) => {
							info!(logger, "Received invalid message from {}", addr);
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
