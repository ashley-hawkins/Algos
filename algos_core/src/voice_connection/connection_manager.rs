use std::sync::Arc;

use discortp::rtp::{RtpPacket, RtpType};
use slog::{info, warn};
use tokio::{select, sync::oneshot};

use super::{
	structures::IpDiscoveryPacket,
	udp_connection::{ConnectionHandle, VoiceConnMessage},
	user_manager::UserManagerHandle,
};

use crate::{
	constants, crypt::VoiceConnectionCrypt, voice_connection::user_manager::UserManagerMessage,
	SyncMutex,
};

#[derive(Debug)]
pub enum ConnectionManagerMessage {
	Ping { seq: u8, respond_to: oneshot::Sender<()> },
	IpDiscovery { data: IpDiscoveryPacket, respond_to: oneshot::Sender<IpDiscoveryPacket> },
	Rtp { data: Vec<u8> },
}

#[derive(Clone)]
pub struct ConnectionManagerHandle {
	pub outbound: flume::Sender<ConnectionManagerMessage>,
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
		let (outbound_tx, outbound_rx) =
			flume::bounded::<ConnectionManagerMessage>(constants::MAIN_CHANNELS_SIZE);
		tokio::spawn(async move {
			loop {
				select! {
					Ok(msg) = connection.inbound.recv_async() => {
						self.process_inbound_message(msg, &mut user_manager);
					},
					Ok(msg) = outbound_rx.recv_async() => {
						// println!("Received message to send out: {:#?}", msg);
						self.process_outbound_message(msg, &mut connection, &mut user_manager).await;
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
			VoiceConnMessage::Rtp(mut data) => {
				let rtp_packet_view = RtpPacket::new(&data).unwrap();

				let payload_type = rtp_packet_view.get_payload_type();

				// If this isn't an Opus packet, ignore it
				if !matches!(payload_type, RtpType::Dynamic(120)) {
					return;
				}

				let ssrc = rtp_packet_view.get_ssrc();

				let mut crypt = self.crypt.lock();
				if let Some((header_length, total_length)) =
					crypt.decrypt_in_place(data.as_mut_slice())
				{
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

	async fn process_outbound_message(
		&mut self,
		msg: ConnectionManagerMessage,
		connection: &mut ConnectionHandle,
		user_manager: &mut UserManagerHandle,
	) {
		match msg {
			ConnectionManagerMessage::Ping { seq, respond_to } => {
				self.last_ping_respond_to = Some((seq, respond_to));
				if let Err(e) = connection.outbound.send_async(VoiceConnMessage::Ping { seq }).await
				{
					warn!(self.logger, "Failed to send ping message: {e}");
				};
			}
			ConnectionManagerMessage::IpDiscovery { data, respond_to } => {
				self.last_ip_discovery_respond_to = Some(respond_to);
				if let Err(e) =
					connection.outbound.send_async(VoiceConnMessage::IpDiscovery(data)).await
				{
					warn!(self.logger, "Failed to send IP discovery message: {e}");
				}
			}
			ConnectionManagerMessage::Rtp { mut data } => {
				self.crypt.lock().encrypt_in_place(data.as_mut_slice());

				if let Err(e) = connection.outbound.send_async(VoiceConnMessage::Rtp(data)).await {
					warn!(self.logger, "Failed to send RTP message: {e}");
				}
			}
		}
	}
}
