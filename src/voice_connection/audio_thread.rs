use std::sync::Arc;

use cpal::OutputCallbackInfo;
use ringbuf::traits::{Consumer, Observer};

use super::user_manager::UserCommon;

pub struct AudioThreadUser {
	user_id: u64,
	draining: bool,
	common: Arc<UserCommon>,
	reader: ringbuf::HeapCons<f32>,
}

impl AudioThreadUser {
	pub fn new(user_id: u64, common: Arc<UserCommon>, reader: ringbuf::HeapCons<f32>) -> Self {
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
		let (message_sender, message_receiver) = flume::bounded(256);

		let mut this = Self { users: Vec::new() };

		(AudioThreadHandle { message_sender }, move |data, info| {
			this.data_callback(&message_receiver, data, info)
		})
	}

	pub fn data_callback(
		&mut self,
		message_receiver: &flume::Receiver<AudioThreadMessage>,
		data: &mut [f32],
		callback_info: &OutputCallbackInfo,
	) {
		message_receiver.try_iter().for_each(|message| self.process_message(message));

		for sample in data.iter_mut() {
			*sample = 0.0;
		}

		for user in self.users.iter_mut() {
			let available = user.reader.occupied_len();

			if available > 5760 * 2 {
				user.draining = true;
			}
			if (available < data.len() * 2) {
				user.draining = false;
			}

			if !user.draining {
				continue;
			}

			let mut our_data = [0.0; 5760 * 2 * 10];

			let data_amount = user.reader.pop_slice(&mut our_data);

			for (dst, src) in data.iter_mut().zip(&our_data[..data_amount]) {
				*dst += src;
			}
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
