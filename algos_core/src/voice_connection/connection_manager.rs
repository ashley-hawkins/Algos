use std::sync::Arc;

use slog::{info, warn};
use tokio::{select, sync::oneshot};

use super::{
	structures::{IpDiscoveryPacket, RtpPacket, RtpPacketTrait},
	udp_connection::{ConnectionHandle, VoiceConnMessage},
	user_manager::UserManagerHandle,
};

use crate::{
	crypt::VoiceConnectionCrypt, voice_connection::user_manager::UserManagerMessage, SyncMutex,
};

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

pub struct ConnectionManager {
	crypt: Arc<SyncMutex<VoiceConnectionCrypt>>,
	logger: slog::Logger,
	last_ping_respond_to: Option<(u8, oneshot::Sender<()>)>,
	last_ip_discovery_respond_to: Option<oneshot::Sender<IpDiscoveryPacket>>,
}

impl ConnectionManager {
	pub fn new(logger: slog::Logger, crypt: Arc<SyncMutex<VoiceConnectionCrypt>>) -> Self {
		Self { crypt, logger, last_ping_respond_to: None, last_ip_discovery_respond_to: None }
	}

	pub fn start(
		mut self,
		mut connection: ConnectionHandle,
		mut user_manager: UserManagerHandle,
	) -> ConnectionManagerHandle {
		let (outbound_tx, mut outbound_rx) =
			tokio::sync::mpsc::unbounded_channel::<ConnectionManagerMessage>();
		tokio::spawn(async move {
			loop {
				select! {
					Some(msg) = connection.inbound.recv() => {
						self.process_inbound_message(msg, &mut user_manager);
					},
					Some(msg) = outbound_rx.recv() => {
						// println!("Received message to send out: {:#?}", msg);
						self.process_outbound_message(msg, &mut connection);
					},
					else => {
						info!(self.logger, "Connection manager closed.");
						break;
					}
				}
			}
		});
		ConnectionManagerHandle { outbound: outbound_tx }
	}

	fn process_inbound_message(
		&mut self,
		msg: VoiceConnMessage,
		user_manager: &mut UserManagerHandle,
	) {
		match msg {
			VoiceConnMessage::Ping { seq } => {
				if let Some((want_seq, respond_to)) = self.last_ping_respond_to.take() {
					if want_seq == seq {
						if respond_to.send(()).is_err() {
							warn!(self.logger, "Failed to send ping response back to caller.");
						}
					} else {
						self.last_ping_respond_to = Some((want_seq, respond_to));
					}
				}
			}
			VoiceConnMessage::IpDiscovery(data) => {
				if let Some(respond_to) = self.last_ip_discovery_respond_to.take() {
					// TODO: Handle fail
					if respond_to.send(data).is_err() {
						warn!(self.logger, "Failed to send IP discovery response back to caller.");
					};
				}
			}
			VoiceConnMessage::Rtp(data) => {
				// TODO: Handle receiving RTP packets
				let payload_type = data.payload_type();

				if payload_type != 120 {
					return;
				}

				let ssrc = data.ssrc();
				let mut data = data.into_raw();

				let crypt = self.crypt.lock();
				if let Some((header_length, total_length)) = crypt.decrypt_in_place(&mut data) {
					// info!(self.logger, "Received RTP packet. Header length: {header_length}, Total length: {total_length}, Ssrc: {ssrc}, Packet type: {payload_type}");

					let _ = user_manager
						.message_sender()
						// TODO: this is probably not super efficient idk
						.try_send(UserManagerMessage::Audio(
							ssrc,
							data[header_length..total_length].to_vec(),
						));
					// info!(self.logger, "Decoded audio packet: {}", output.len());
				} else {
					warn!(self.logger, "Failed to decrypt RTP packet. Ssrc: {ssrc}");
				}
			}
		}
	}

	fn process_outbound_message(
		&mut self,
		msg: ConnectionManagerMessage,
		connection: &mut ConnectionHandle,
	) {
		match msg {
			ConnectionManagerMessage::Ping { seq, respond_to } => {
				self.last_ping_respond_to = Some((seq, respond_to));
				if let Err(e) = connection.outbound.send(VoiceConnMessage::Ping { seq }) {
					warn!(self.logger, "Failed to send ping message: {e}");
				};
			}
			ConnectionManagerMessage::IpDiscovery { data, respond_to } => {
				self.last_ip_discovery_respond_to = Some(respond_to);
				if let Err(e) = connection.outbound.send(VoiceConnMessage::IpDiscovery(data)) {
					warn!(self.logger, "Failed to send IP discovery message: {e}");
				}
			}
			ConnectionManagerMessage::Rtp { data } => {
				if let Err(e) = connection.outbound.send(VoiceConnMessage::Rtp(data)) {
					warn!(self.logger, "Failed to send RTP message: {e}");
				}
			}
		}
	}
}
