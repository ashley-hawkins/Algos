use std::sync::{
	atomic::{self, AtomicU32},
	Arc,
};

use atomic_float::AtomicF32;
use rtrb::CopyToUninit;
use serde::Deserialize;
use serde_with::serde_as;

use crate::constants;

use super::audio_thread::{self, AudioOutStateHandle, AudioOutUser};

pub struct RemoteUserCommon {
	pub ssrc: AtomicU32,
	pub volume: AtomicF32,
}

pub struct RemoteUser {
	user_id: u64,
	common: Arc<RemoteUserCommon>,
	writer: rtrb::Producer<f32>,

	decoder: opus::Decoder,
}

impl RemoteUser {
	pub fn create_pair(user_id: u64, ssrc: u32, vol: f32) -> (RemoteUser, AudioOutUser) {
		let common =
			Arc::new(RemoteUserCommon { ssrc: AtomicU32::new(ssrc), volume: AtomicF32::new(vol) });

		let (rb_tx, rb_rx) = rtrb::RingBuffer::new(48000 * 2 * 5);

		(
			RemoteUser {
				user_id,
				common: common.clone(),
				writer: rb_tx,
				decoder: opus::Decoder::new(48000, opus::Channels::Stereo).unwrap(),
			},
			AudioOutUser::new(user_id, common, rb_rx),
		)
	}

	pub fn user_id(&self) -> u64 {
		self.user_id
	}

	pub fn common(&self) -> &Arc<RemoteUserCommon> {
		&self.common
	}
}

#[derive(Clone)]
pub struct UserManagerHandle {
	message_sender: flume::Sender<UserManagerMessage>,
}

impl UserManagerHandle {
	pub fn message_sender(&self) -> &flume::Sender<UserManagerMessage> {
		&self.message_sender
	}
}

pub enum UserManagerMessage {
	MergeUsers(Vec<UserInitialData>),
	DestroyUser(u64),
	SetVolume(u64, f32),
	Audio(u32, Vec<u8>),
}

#[allow(dead_code)]
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInitialData {
	#[serde_as(as = "serde_with::DisplayFromStr")]
	id: u64,
	mute: bool,
	rtx_ssrc: u32,
	ssrc: u32,
	video_ssrc: u32,
	video_ssrcs: Vec<u32>,
	volume: f32,
}

pub struct UserManager {
	users: Vec<RemoteUser>,
	audio_thread: AudioOutStateHandle,
}

impl UserManager {
	pub fn new(audio_thread: AudioOutStateHandle) -> Self {
		Self { users: Vec::new(), audio_thread }
	}

	pub fn start(mut self) -> UserManagerHandle {
		let (message_sender, message_receiver) = flume::bounded(constants::MAIN_CHANNELS_SIZE);

		tokio::spawn(async move {
			while let Ok(msg) = message_receiver.recv_async().await {
				self.process_message(msg);
			}
		});

		UserManagerHandle { message_sender }
	}

	fn process_message(&mut self, msg: UserManagerMessage) {
		match msg {
			UserManagerMessage::MergeUsers(users) => {
				let (users, audio_thread_users): (Vec<_>, Vec<_>) = users
					.into_iter()
					.filter_map(|new_user| {
						if let Some(existing_user) =
							self.users.iter_mut().find(|u| u.user_id() == new_user.id)
						{
							existing_user
								.common()
								.ssrc
								.store(new_user.ssrc, atomic::Ordering::SeqCst);

							return None;
						}

						let pair = RemoteUser::create_pair(new_user.id, new_user.ssrc, new_user.volume);
						Some(pair)
					})
					.unzip();

				self.users.extend(users);

				match audio_thread_users.len() {
					0 => {}
					1 => {
						self.audio_thread
							.message_sender()
							.send(audio_thread::AudioOutStateMessage::AddUser(
								audio_thread_users.into_iter().next().unwrap(),
							))
							.expect("Failed to send message to audio thread");
					}
					2.. => self
						.audio_thread
						.message_sender()
						.send(audio_thread::AudioOutStateMessage::AddUsers(audio_thread_users))
						.expect("Failed to send message to audio thread"),
				}
			}
			UserManagerMessage::DestroyUser(user_id) => {
				self.users.retain(|user| user.user_id() != user_id);
				self.audio_thread
					.message_sender()
					.send(audio_thread::AudioOutStateMessage::RemoveUser(user_id))
					.expect("Failed to send message to audio thread");
			}
			UserManagerMessage::SetVolume(user_id, volume) => {
				if let Some(user) = self.users.iter().find(|user| user.user_id() == user_id) {
					user.common().volume.store(volume, atomic::Ordering::Relaxed);
				}
			}
			UserManagerMessage::Audio(ssrc, data) => {
				if let Some(user) = self
					.users
					.iter_mut()
					.find(|user| user.common().ssrc.load(atomic::Ordering::SeqCst) == ssrc)
				{
					let mut output = [0.0; 5760 * 2];

					let writer = &mut user.writer;

					match user.decoder.decode_float(&data, &mut output, false) {
						Ok(len) => {
							let len = len * 2;
							if let Ok(mut chunk) = writer.write_chunk_uninit(len) {
								let (first, second) = chunk.as_mut_slices();
								let mid = first.len();
								output[..mid].copy_to_uninit(first);
								output[mid..len].copy_to_uninit(second);
								// SAFETY: All slots have been initialized
								unsafe { chunk.commit_all() };
							}
						}
						Err(_e) => {
							panic!("Sneed to handle this error");
							// warn!(self.logger, "Failed to decode opus packet: {e}; {ssrc} {csrc_count} {total_length} {header_length} {has_ext} {ext_id:?} {ext_len:?} {ext_payload:?} Data: {data:?} Data Original: {data_original:?}");
						}
					};
				}
			}
		}
	}
}
