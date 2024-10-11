use std::sync::{atomic::AtomicBool, Arc};

use atomic_float::AtomicF32;
use cpal::OutputCallbackInfo;

use super::user_manager::{try_write, RemoteUserCommon};

pub struct AudioOutUser {
	user_id: u64,
	draining: bool,
	common: Arc<RemoteUserCommon>,
	reader: rtrb::Consumer<f32>,
}

impl AudioOutUser {
	pub fn new(user_id: u64, common: Arc<RemoteUserCommon>, reader: rtrb::Consumer<f32>) -> Self {
		Self { user_id, draining: false, common, reader }
	}
}

pub enum AudioOutStateMessage {
	AddUser(AudioOutUser),
	RemoveUser(u64),
	AddUsers(Vec<AudioOutUser>),
	RemoveUsers(Vec<u64>),
}

pub struct AudioOutStateHandle {
	message_sender: flume::Sender<AudioOutStateMessage>,
}

impl AudioOutStateHandle {
	pub fn message_sender(&self) -> flume::Sender<AudioOutStateMessage> {
		self.message_sender.clone()
	}
}

pub struct AudioOutState {
	users: Vec<AudioOutUser>,
}

impl AudioOutState {
	pub fn create_callback() -> (AudioOutStateHandle, impl FnMut(&mut [f32], &OutputCallbackInfo)) {
		let (message_sender, message_receiver) = flume::bounded(2);

		let mut this = Self { users: Vec::new() };

		(AudioOutStateHandle { message_sender }, move |data, info| {
			this.data_callback(&message_receiver, data, info)
		})
	}

	fn data_callback(
		&mut self,
		message_receiver: &flume::Receiver<AudioOutStateMessage>,
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

	fn process_message(&mut self, message: AudioOutStateMessage) {
		match message {
			AudioOutStateMessage::AddUser(user) => {
				self.users.push(user);
			}
			AudioOutStateMessage::RemoveUser(user_id) => {
				self.users.retain(|user| user.user_id != user_id);
			}
			AudioOutStateMessage::AddUsers(users) => {
				self.users.extend(users);
			}
			AudioOutStateMessage::RemoveUsers(user_ids) => {
				self.users.retain(|user| !user_ids.contains(&user.user_id));
			}
		}
	}
}

pub struct LocalUserCommon {
	pub volume: AtomicF32,
	pub muted: AtomicBool,
}

pub struct AudioInState {
	common: Arc<LocalUserCommon>,
	writer: rtrb::Producer<f32>,
}

impl AudioInState {
	pub fn create_callback() -> (rtrb::Consumer<f32>, impl FnMut(&[f32], &cpal::InputCallbackInfo))
	{
		let (writer, reader) = rtrb::RingBuffer::new(5760 * 20);
		let common = Arc::new(LocalUserCommon {
			volume: AtomicF32::new(1.0),
			muted: AtomicBool::new(false),
		});

		let mut this = Self { writer, common };

		(reader, move |data, _info| this.data_callback(data))
	}

	fn data_callback(&mut self, data: &[f32]) {
		try_write(&mut self.writer, data);
	}
}
