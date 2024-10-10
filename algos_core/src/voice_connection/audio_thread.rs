use std::sync::Arc;

use cpal::OutputCallbackInfo;

use super::user_manager::UserCommon;

pub struct AudioThreadUser {
	user_id: u64,
	draining: bool,
	common: Arc<UserCommon>,
	reader: rtrb::Consumer<f32>,
}

impl AudioThreadUser {
	pub fn new(user_id: u64, common: Arc<UserCommon>, reader: rtrb::Consumer<f32>) -> Self {
		Self { user_id, draining: false, common, reader }
	}
}

pub enum AudioThreadMessage {
	AddUser(AudioThreadUser),
	RemoveUser(u64),
	AddUsers(Vec<AudioThreadUser>),
	RemoveUsers(Vec<u64>),
}

pub struct AudioThreadHandle {
	message_sender: flume::Sender<AudioThreadMessage>,
}

impl AudioThreadHandle {
	pub fn message_sender(&self) -> flume::Sender<AudioThreadMessage> {
		self.message_sender.clone()
	}
}

pub struct AudioThreadState {
	users: Vec<AudioThreadUser>,
}

impl AudioThreadState {
	pub fn create_callback() -> (AudioThreadHandle, impl FnMut(&mut [f32], &OutputCallbackInfo)) {
		let (message_sender, message_receiver) = flume::bounded(2);

		let mut this = Self { users: Vec::new() };

		(AudioThreadHandle { message_sender }, move |data, info| {
			this.data_callback(&message_receiver, data, info)
		})
	}

	pub fn data_callback(
		&mut self,
		message_receiver: &flume::Receiver<AudioThreadMessage>,
		data: &mut [f32],
		_callback_info: &OutputCallbackInfo,
	) {
		loop {
			let x = message_receiver.try_recv();

			match x {
				Ok(x) => {
					self.process_message(x);
				}
				Err(flume::TryRecvError::Empty) => break,
				Err(flume::TryRecvError::Disconnected) => panic!(),
			}
		}

		for sample in data.iter_mut() {
			*sample = 0.0;
		}

		let wanted = data.len();

		for user in self.users.iter_mut() {
			let available = user.reader.slots();
			let vol = user.common.volume.load(std::sync::atomic::Ordering::Relaxed).clamp(0.0, 1.0);

			if available > 5760 {
				user.draining = true;
			}

			if available < wanted {
				user.draining = false;
			}

			if !user.draining {
				continue;
			}

			user.reader.read_chunk(data.len()).unwrap().into_iter().zip(data.iter_mut()).for_each(
				|(src, dst)| {
					*dst += vol * src;
				},
			);
		}
	}

	pub fn process_message(&mut self, message: AudioThreadMessage) {
		match message {
			AudioThreadMessage::AddUser(user) => {
				self.users.push(user);
			}
			AudioThreadMessage::RemoveUser(user_id) => {
				self.users.retain(|user| user.user_id != user_id);
			}
			AudioThreadMessage::AddUsers(users) => {
				self.users.extend(users);
			}
			AudioThreadMessage::RemoveUsers(user_ids) => {
				self.users.retain(|user| !user_ids.contains(&user.user_id));
			}
		}
	}
}
