use std::{
	cell::RefCell,
	collections::HashMap,
	net::{Ipv4Addr, UdpSocket},
	rc::Rc,
	sync::Arc,
};

use gst::{bus::BusWatchGuard, prelude::*};

use discord_electron::{deliver_discord_frame, DiscordFrame, DiscordYuvFrame};
use glib::object::Cast;
use slog::info;
use tokio::sync::oneshot;

pub enum VideoThreadCommand {
	ReserveStream { reply_to: oneshot::Sender<u64> },
	CreateStream { stream_id: u64 },
	DestroyStream { stream_id: u64 },
	Packet { stream_id: u64, packet: Vec<u8> },
}

#[derive(Clone)]
pub struct VideoThreadHandle {
	pub sender: flume::Sender<VideoThreadCommand>,
}

pub fn run_video_thread(logger: slog::Logger) -> VideoThreadHandle {
	let (sender, receiver) = flume::unbounded();

	let udp_sender = UdpSocket::bind("127.0.0.1:0").unwrap();

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
					add_stream(logger.clone(), stream_id, &mut streams);
				}
				VideoThreadCommand::DestroyStream { stream_id } => {
					println!("Destroying stream in video thread");
					streams.remove(&stream_id);
				}
				VideoThreadCommand::Packet { stream_id, packet } => {
					if let Some(Some((sender, _))) = streams.get_mut(&stream_id) {
						// sender.send(packet).unwrap();
						let res =
							udp_sender.send_to(&packet, (Ipv4Addr::new(127, 0, 0, 1), *sender));

						//

						// let buf = gst::Buffer::with_size(packet.len()).unwrap();

						// let mut buf = buf.into_mapped_buffer_writable().unwrap();
						// buf.copy_from_slice(&packet);

						// let mut buf = buf.into_buffer();
						// // {
						// // 	let buf = buf.get_mut().unwrap();
						// // 	buf.set_size(packet.len());
						// // 	buf.set_pts(gst::ClockTime::from_mseconds(
						// // 		begin.elapsed().as_millis() as u64
						// // 	));
						// // }

						// let _ = sender.push_buffer(buf);

						// //
					}
				}
			});
			glib::ControlFlow::Continue
		});

		let main_loop = glib::MainLoop::new(None, false);
		gst::init().unwrap();
		main_loop.run();
	});

	VideoThreadHandle { sender }
}

// fn add_stream(this_stream_id: u64, streams: &mut HashMap<u64, Option<(flume::Sender<Vec<u8>>, gst::Element)>>) {
fn add_stream(
	logger: slog::Logger,
	this_stream_id: u64,
	streams: &mut HashMap<u64, Option<(u16, (gst::Element, BusWatchGuard))>>,
) {
	println!("Adding stream to video thread");

	// let e = gst::parse::launch(
	//                         r##"udpsrc name=src port=0 caps="application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)H264, payload=(int)96" ! rtph264depay ! decodebin ! video/x-raw,format=(string)I420,framerate=30/1,width=(int)1280,height=(int)720 ! appsink emit-signals=true name=dst"##,
	//                     ).unwrap();
	// let e = gst::parse::launch(
	//     r##"appsrc is-live="true" name=src caps="application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)VP8, payload=(int)105" ! rtpvp8depay ! decodebin ! videoconvert ! appsink emit-signals=true name=dst caps="video/x-raw,format=(string)I420,framerate=30/1,width=(int)1280,height=(int)720""##,
	// ).unwrap();
	// let e: gst::Element = gst::parse::launch(
	//     r##"appsrc is-live="true" name=src caps="application/x-rtp, media=video, clock-rate=90000" ! rtpvp8depay ! fakesink"##,
	// ).unwrap();

	let e = gst::parse::launch(
        r##"udpsrc name=src caps = "application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)H264, payload=(int)96" port=0 ! rtph264depay ! decodebin ! videoscale ! videoconvert ! video/x-raw,format=I420,width=1280,height=720 ! appsink emit-signals=true name=dst"##,
                    ).unwrap();

	let e: gst::Bin = e.downcast().unwrap();
	// let app_src: gst_app::AppSrc = e.by_name("src").unwrap().downcast().unwrap();
	let udp_src: gst_base::PushSrc = e.by_name("src").unwrap().downcast().unwrap();
	let app_sink: gst_app::AppSink = e.by_name("dst").unwrap().downcast().unwrap();

	let mut timestamp_us: u64 = 0;
	let sink_callbacks = gst_app::AppSinkCallbacks::builder()
		.eos(|_| {})
		.new_preroll(move |_| Ok(gst::FlowSuccess::Ok))
		.new_sample({
			let logger = logger.clone();
			move |sink| {
				info!(logger, "NEWSAMPLE");
				let sample = Arc::new(sink.pull_sample().unwrap());
				let buf = sample.buffer().unwrap();
				let sample_mem = buf.all_memory().unwrap();
				let mapped_memory = sample_mem.into_mapped_memory_readable().unwrap();

				let width = 1280;
				let height = 720;

				let yuv_frame = DiscordYuvFrame::new(mapped_memory, width, height);

				let frame = DiscordFrame {
					timestamp_us: timestamp_us as i64,
					frame: yuv_frame,
					width: width as i32,
					height: height as i32,
				};

				timestamp_us = timestamp_us.wrapping_add(1_000_000 / 30);

				unsafe { deliver_discord_frame(&this_stream_id.to_string(), frame) };

				Ok(gst::FlowSuccess::Ok)
			}
		})
		.build();

	app_sink.set_callbacks(sink_callbacks);

	// let (packet_sender, packet_receiver) = flume::unbounded::<Vec<u8>>();

	let had_enough = false;
	// let begin = std::time::Instant::now();
	// let src_callbacks = AppSrcCallbacks::builder()
	// 	.need_data(move |x, want_bytes| {
	// for packet in packet_receiver.try_iter() {
	// 	let buf = gst::Buffer::with_size(packet.len()).unwrap();

	// 	let mut buf = buf.into_mapped_buffer_writable().unwrap();
	// 	buf.copy_from_slice(&packet);

	// 	let mut buf = buf.into_buffer();
	// 	{
	// 		let buf = buf.get_mut().unwrap();
	// 		buf.set_size(packet.len());
	// 		buf.set_pts(gst::ClockTime::from_mseconds(begin.elapsed().as_millis() as u64));
	// 	}

	// 	let _ = x.push_buffer(buf);
	// 		}
	// 	})
	// 	.build();
	// app_src.set_callbacks(src_callbacks);

	e.set_state(gst::State::Playing).unwrap();
	let guard = e
		.bus()
		.unwrap()
		.add_watch(move |_bus, message| {
			let view = message.view();
			info!(logger, "BUSMESSAGE: {view:#?}");

			glib::ControlFlow::Continue
		})
		.unwrap();
	let port = udp_src.property::<i32>("port") as u16;
	// streams.insert(this_stream_id, Some((packet_sender, e.upcast())));
	streams.insert(this_stream_id, Some((port, (e.upcast(), guard))));
}
