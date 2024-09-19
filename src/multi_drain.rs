// use std::{
// 	error::Error,
// 	sync::{Arc, Mutex},
// };

// use slog::Drain;

// trait OurDrain: Send + Drain<Ok = (), Err = Box<dyn Error>> {}
// impl<T> OurDrain for T where T: Send + Drain<Ok = (), Err = Box<dyn Error>> {}

// struct MultiDrainInternal {
// 	drains: Vec<Box<dyn OurDrain>>,
// }

// #[derive(Clone)]
// struct MultiDrain {
// 	internal: Arc<Mutex<MultiDrainInternal>>,
// }

// impl MultiDrain {
// 	fn new() -> Self {
// 		Self { internal: Arc::new(Mutex::new(MultiDrainInternal { drains: Vec::new() })) }
// 	}
// 	fn push<D: 'static + OurDrain>(&self, drain: D) {
// 		self.internal.lock().unwrap().drains.push(Box::new(drain));
// 	}
// }

// impl Drain for MultiDrain {
//   type Ok = ();
//   type Err = slog::Never;

//   fn log(&self, record: &Record, logger_values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
//     for drain in &self.internal.lock().unwrap().drains {
//       drain.log(record, logger_values)?;
//     }
//     Ok(())
//   }
// }
