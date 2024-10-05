use std::sync::{
	atomic::{self, AtomicU32},
	Arc,
};

use ringbuf::traits::{Producer, Split};
use serde::Deserialize;
use serde_with::serde_as;

use super::audio_thread::{self, AudioThreadHandle, AudioThreadUser};

pub struct UserCommon {
	pub ssrc: AtomicU32,
	pub volume: AtomicU32,
}

pub struct User {
	user_id: u64,
	common: Arc<UserCommon>,
	writer: rtrb::Producer<f32>,
}

impl User {
	pub fn create_pair(user_id: u64, ssrc: u32) -> (User, AudioThreadUser) {
		let common = Arc::new(UserCommon { ssrc: AtomicU32::new(0), volume: AtomicU32::new(100) });

		let (rb_tx, rb_rx) = rtrb::RingBuffer::new(5760 * 2 * 10);

		(
			User { user_id, common: common.clone(), writer: rb_tx },
			AudioThreadUser::new(user_id, common, rb_rx),
		)
	}

	pub fn user_id(&self) -> u64 {
		self.user_id
	}

	pub fn common(&self) -> &Arc<UserCommon> {
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
	Audio(u32, Vec<f32>),
}

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
	users: Vec<User>,
	audio_thread: AudioThreadHandle,
}

impl UserManager {
	pub fn new(audio_thread: AudioThreadHandle) -> Self {
		Self { users: Vec::new(), audio_thread }
	}

	pub fn start(mut self) -> UserManagerHandle {
		let (message_sender, message_receiver) = flume::bounded(8);

		tokio::spawn(async move {
			while let Ok(msg) = message_receiver.recv_async().await {
				self.process_message(msg).await;
			}
		});

		UserManagerHandle { message_sender }
	}

	async fn process_message(&mut self, msg: UserManagerMessage) {
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

						let pair = User::create_pair(new_user.id, new_user.ssrc);
						Some(pair)
					})
					.unzip();

				self.users.extend(users);

				match audio_thread_users.len() {
					0 => {}
					1 => {
						self.audio_thread
							.message_sender()
							.send_async(audio_thread::AudioThreadMessage::AddUser(
								audio_thread_users.into_iter().next().unwrap(),
							))
							.await
							.expect("Failed to send message to audio thread");
					}
					2.. => self
						.audio_thread
						.message_sender()
						.send_async(audio_thread::AudioThreadMessage::AddUsers(audio_thread_users))
						.await
						.expect("Failed to send message to audio thread"),
				}
			}
			UserManagerMessage::DestroyUser(user_id) => {
				self.users.retain(|user| user.user_id() != user_id);
				self.audio_thread
					.message_sender()
					.send_async(audio_thread::AudioThreadMessage::RemoveUser(user_id))
					.await
					.expect("Failed to send message to audio thread");
			}
			UserManagerMessage::Audio(ssrc, data) => {
				if let Some(user) = self
					.users
					.iter_mut()
					.find(|user| user.common().ssrc.load(atomic::Ordering::SeqCst) == ssrc)
				{
					let writer = &mut user.writer;

					//writer.push_iter(data.chunks_exact(2).map(|chunk| (chunk[0], chunk[1])));
					writer.write_chunk_uninit(data.len()).unwrap().fill_from_iter(data.into_iter());
				}
			}
		}
	}
}
