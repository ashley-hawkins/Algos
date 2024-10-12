use std::{cell::RefCell, collections::HashMap, convert::identity, rc::Rc, sync::Arc};

use glib::object::{Cast, ObjectExt};
use gstreamer::prelude::*;
use tokio::sync::oneshot;

use crate::discord_electron::{
	deliver_discord_frame, DiscordFrame, DiscordFrameType, DiscordFrameUnion, DiscordYUVFrame,
};

pub enum VideoThreadCommand {
	ReserveStream { reply_to: oneshot::Sender<u64> },
	CreateStream { stream_id: u64 },
	DestroyStream { stream_id: u64 },
}

#[derive(Clone)]
pub struct VideoThreadHandle {
	pub sender: flume::Sender<VideoThreadCommand>,
}

pub fn run_video_thread() -> VideoThreadHandle {
	let (sender, receiver) = flume::unbounded();

	std::thread::spawn(move || {
		let main_context = glib::MainContext::default();
		let _main_context_guard = main_context.acquire().unwrap();

		let next_stream_id = Rc::new(RefCell::new(0));

		let mut streams = HashMap::new();

		let idle = glib::idle_add_local(move || {
			receiver.try_recv().ok().map(|cmd| match cmd {
				VideoThreadCommand::ReserveStream { reply_to } => {
					let this_stream_id = *next_stream_id.borrow();

					*next_stream_id.borrow_mut() += 1;

					streams.insert(this_stream_id, None);

					reply_to.send(this_stream_id).unwrap();
				}
				VideoThreadCommand::CreateStream { stream_id } => {
					add_stream(stream_id, &mut streams);
				}
				VideoThreadCommand::DestroyStream { stream_id } => {
					println!("Destroying stream in video thread");
					streams.remove(&stream_id);
				}
			});
			glib::ControlFlow::Continue
		});
        
		let main_loop = glib::MainLoop::new(None, false);
        gstreamer::init().unwrap();
        main_loop.run();
	});

	VideoThreadHandle { sender }
}

struct MemWrapper<'a, T> {
	sample_mem: Arc<gstreamer::Memory>,
	map: gstreamer::MemoryMap<'a, T>,
}

fn add_stream(this_stream_id: u64, streams: &mut HashMap<u64, Option<gstreamer::Element>>) {
	println!("Adding stream to video thread");

	let e = gstreamer::parse::launch(
                            r##"videotestsrc ! video/x-raw,format=(string)I420,framerate=30/1,width=(int)1280,height=(int)720 ! appsink emit-signals=true name=a"##,
                        ).unwrap();

	let e: gstreamer::Bin = e.downcast().unwrap();
	let app = e.by_name("a").unwrap();
	let app_sink: gstreamer_app::AppSink = app.downcast().unwrap();

	let mut timestamp_us: u64 = 0;
	let callbacks = gstreamer_app::AppSinkCallbacks::builder()
		.eos(|_| {})
		.new_preroll(move |_| Ok(gstreamer::FlowSuccess::Ok))
		.new_sample(move |sink| {
			let sample = Arc::new(sink.pull_sample().unwrap());
			let buf = sample.buffer().unwrap();
			let sample_mem = buf.all_memory().unwrap();
			let map = sample_mem.map_readable().unwrap();

			let width = 1280;
			let height = 720;

			let yuv_frame = unsafe {
				DiscordYUVFrame::from_raw_unchecked(map.as_slice().as_ptr(), width, height)
			};

			drop(map);

			let frame = DiscordFrame {
				timestamp_us: timestamp_us as i64,
				frame: DiscordFrameUnion { yuv: yuv_frame },
				width: width as i32,
				height: height as i32,
				type_: DiscordFrameType::DiscordFrameI420,
			};

			timestamp_us = timestamp_us.wrapping_add(1_000_000 / 30);

			unsafe {
				deliver_discord_frame(
					&this_stream_id.to_string(),
					frame,
					{
						move || {
							drop(sample_mem);
						}
					},
					std::ptr::null_mut(),
				);
			}

			Ok(gstreamer::FlowSuccess::Ok)
		})
		.build();

	app_sink.set_callbacks(callbacks);
    
    e.set_state(gstreamer::State::Playing).unwrap();
	streams.insert(this_stream_id, Some(e.upcast()));
}
