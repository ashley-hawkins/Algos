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
	progress: f32,
}

impl AudioThreadState {
	pub fn create_callback() -> (AudioThreadHandle, impl FnMut(&mut [f32], &OutputCallbackInfo)) {
		let (message_sender, message_receiver) = flume::bounded(256);

		let mut this = Self { users: Vec::new(), progress: 0.0 };

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
		// const SAMPLE_RATE: f32 = 48_000.0;
		// const DELTA_T: f32 = 1.0 / SAMPLE_RATE;
		// const FREQ: f32 = 880.0;
		// const AMPLITUDE: f32 = 0.1;

		message_receiver.try_iter().for_each(|message| self.process_message(message));

		for sample in data.iter_mut() {
			*sample = 0.0;
		}

		// sanity check by playing a sine wave
		// // progress through one second
		// let mut prog = self.progress;

		// for (samples) in data.chunks_exact_mut(2) {
		// 	let a = AMPLITUDE * (prog * std::f32::consts::PI * FREQ).sin();

		// 	samples[0] = a;
		// 	samples[1] = a;

		// 	prog = (prog + DELTA_T) % 1.0;
		// }

		// self.progress = prog;

		let wanted = data.len();

		for user in self.users.iter_mut() {
			let available = user.reader.slots();
			if available > 5760 * 2 {
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
					*dst += src;
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
