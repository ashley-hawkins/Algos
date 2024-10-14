use std::sync::{
	atomic::{self},
	Arc,
};

use atomic_float::AtomicF32;
use rtrb::CopyToUninit;
use serde::Deserialize;
use serde_with::serde_as;
use tokio::sync::{oneshot, watch};

use crate::{
	constants,
	video_thread::{VideoThreadCommand, VideoThreadHandle},
};

use super::audio_thread::{self, AudioOutStateHandle, AudioOutUser};

pub struct RemoteUserCommon {
	pub volume: AtomicF32,
}

pub struct RemoteUser {
	user_id: u64,
	ssrc: u32,
	video_ssrc: u32,
	common: Arc<RemoteUserCommon>,
	writer: rtrb::Producer<f32>,
	decoder: opus::Decoder,
	video_stream_id: Option<u64>,
}

impl RemoteUser {
	pub fn create_pair(
		user_id: u64,
		ssrc: u32,
		video_ssrc: u32,
		vol: f32,
	) -> (RemoteUser, AudioOutUser) {
		let common = Arc::new(RemoteUserCommon { volume: AtomicF32::new(vol) });

		let (rb_tx, rb_rx) = rtrb::RingBuffer::new(48000 * 2 * 5);

		(
			RemoteUser {
				user_id,
				ssrc,
				video_ssrc,
				common: common.clone(),
				writer: rb_tx,
				decoder: opus::Decoder::new(48000, opus::Channels::Stereo).unwrap(),
				video_stream_id: None,
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

#[allow(dead_code)]
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInitialData {
	#[serde_as(as = "serde_with::DisplayFromStr")]
	pub id: u64,
	pub mute: bool,
	pub rtx_ssrc: u32,
	pub ssrc: u32,
	pub video_ssrc: u32,
	pub video_ssrcs: Vec<u32>,
	pub volume: f32,
}

#[derive(Clone)]
pub struct UserManagerHandle {
	message_sender: flume::Sender<UserManagerMessage>,
	on_video_callback_sender: watch::Sender<Option<OnVideoCallback>>,
}

impl UserManagerHandle {
	pub fn message_sender(&self) -> &flume::Sender<UserManagerMessage> {
		&self.message_sender
	}

	pub fn set_on_video_callback(&self, f: OnVideoCallback) {
		self.on_video_callback_sender.send(Some(f)).unwrap();
	}
}

pub enum UserManagerMessage {
	MergeUsers(Vec<UserInitialData>),
	DestroyUser(u64),
	SetVolume(u64, f32),
	Audio(u32, Vec<u8>),
	Video(u32, Vec<u8>),
	StreamIdAssigned(u64, u64),
}

type OnVideoCallback = Box<dyn Fn(u64, u32, Option<u64>) + Send + Sync>;

pub struct UserManager {
	users: Vec<RemoteUser>,
	audio_out: AudioOutStateHandle,
	video_thread: VideoThreadHandle,
}

impl UserManager {
	pub fn new(audio_thread: AudioOutStateHandle, video_thread: VideoThreadHandle) -> Self {
		Self { users: Vec::new(), audio_out: audio_thread, video_thread }
	}

	pub fn start(mut self) -> UserManagerHandle {
		let (message_sender, message_receiver) = flume::bounded(constants::MAIN_CHANNELS_SIZE);
		let (tx, mut rx) = watch::channel(None);

		let self_handle = UserManagerHandle { message_sender, on_video_callback_sender: tx };
		tokio::spawn({
			let mut self_handle = self_handle.clone();
			async move {
				// let mut video_udp = UdpSocket::bind("0.0.0.0:0").await.unwrap();
				// video_udp.connect("127.0.0.1:9999").await.unwrap();

				while let Ok(msg) = message_receiver.recv_async().await {
					self.process_message(msg, &mut self_handle, &mut rx).await;
				}
			}
		});

		self_handle
	}

	async fn process_message(
		&mut self,
		msg: UserManagerMessage,
		self_handle: &mut UserManagerHandle,
		video_callback: &mut watch::Receiver<Option<OnVideoCallback>>,
	) {
		match msg {
			UserManagerMessage::MergeUsers(users) => {
				fn handle_video(
					video_thread: &VideoThreadHandle,
					self_handle: &mut UserManagerHandle,
					video_callback: &mut watch::Receiver<
						Option<Box<dyn Fn(u64, u32, Option<u64>) + Send + Sync>>,
					>,
					new_user: &UserInitialData,
				) {
					let (tx, rx) = oneshot::channel();
					video_thread
						.sender
						.send(VideoThreadCommand::ReserveStream { reply_to: tx })
						.unwrap();

					tokio::spawn({
						let self_handle = self_handle.clone();
						let video_callback = video_callback.clone();
						let user_id = new_user.id;
						let ssrc = new_user.ssrc;
						async move {
							let stream_id = rx.await.unwrap();

							if let Some(f) = video_callback.borrow().as_ref() {
								f(user_id, ssrc, Some(stream_id));
							}

							self_handle
								.message_sender()
								.send(UserManagerMessage::StreamIdAssigned(user_id, stream_id))
								.unwrap();
						}
					});
				}

				let (users, audio_thread_users): (Vec<_>, Vec<_>) = users
					.into_iter()
					.filter_map(|new_user| {
						if let Some(existing_user) =
							self.users.iter_mut().find(|u| u.user_id() == new_user.id)
						{
							existing_user.ssrc = new_user.ssrc;
							let initial_video_ssrc = existing_user.video_ssrc;
							existing_user.video_ssrc = new_user.video_ssrc;

							if (initial_video_ssrc == 0) && (new_user.video_ssrc != 0) {
								handle_video(
									&self.video_thread,
									self_handle,
									video_callback,
									&new_user,
								);
							}

							return None;
						}

						if new_user.video_ssrc != 0 {
							handle_video(
								&self.video_thread,
								self_handle,
								video_callback,
								&new_user,
							);
						}

						let pair = RemoteUser::create_pair(
							new_user.id,
							new_user.ssrc,
							new_user.video_ssrc,
							new_user.volume,
						);

						Some(pair)
					})
					.unzip();

				self.users.extend(users);

				match audio_thread_users.len() {
					0 => {}
					1 => {
						self.audio_out
							.message_sender()
							.send(audio_thread::AudioOutStateMessage::AddUser(
								audio_thread_users.into_iter().next().unwrap(),
							))
							.expect("Failed to send message to audio thread");
					}
					2.. => self
						.audio_out
						.message_sender()
						.send(audio_thread::AudioOutStateMessage::AddUsers(audio_thread_users))
						.expect("Failed to send message to audio thread"),
				}
			}
			UserManagerMessage::DestroyUser(user_id) => {
				self.users.retain(|user| user.user_id() != user_id);
				self.audio_out
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
				if let Some(user) = self.users.iter_mut().find(|user| user.ssrc == ssrc) {
					let mut output = [0.0; 5760 * 2];

					let writer = &mut user.writer;

					match user.decoder.decode_float(&data, &mut output, false) {
						Ok(len) => {
							let len = len * 2;
							try_write(writer, &output[..len]);
						}
						Err(_e) => {
							panic!("Sneed to handle this error");
							// warn!(self.logger, "Failed to decode opus packet: {e}; {ssrc} {csrc_count} {total_length} {header_length} {has_ext} {ext_id:?} {ext_len:?} {ext_payload:?} Data: {data:?} Data Original: {data_original:?}");
						}
					};
				}
			}
			UserManagerMessage::Video(ssrc, data) => {
				if let Some(user) = self.users.iter().find(|user| user.video_ssrc == ssrc)
					&& let Some(video_stream_id) = user.video_stream_id
				{
					self.video_thread
						.sender
						.send(VideoThreadCommand::Packet {
							stream_id: video_stream_id,
							packet: data,
						})
						.unwrap();
				}
			}
			UserManagerMessage::StreamIdAssigned(user_id, stream_id) => {
				if let Some(user) = self.users.iter_mut().find(|u| u.user_id() == user_id) {
					user.video_stream_id = Some(stream_id);
				}
			}
		}
	}
}

pub(crate) fn try_write<T: Copy>(writer: &mut rtrb::Producer<T>, data: &[T]) {
	let len = data.len();

	if let Ok(mut chunk) = writer.write_chunk_uninit(len) {
		let (first, second) = chunk.as_mut_slices();
		let mid = first.len();
		data[..mid].copy_to_uninit(first);
		data[mid..len].copy_to_uninit(second);
		// SAFETY: All slots have been initialized
		unsafe { chunk.commit_all() };
	} else {
		panic!("Huh");
	}
}
