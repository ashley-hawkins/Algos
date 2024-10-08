use std::{
	io::{self, Read},
	net::Ipv4Addr,
	str::FromStr,
};

use byteorder::{NetworkEndian, ReadBytesExt};
use static_assertions_next::const_assert;

const_assert!(IpDiscoveryPacket::packet_size() == 74);

#[derive(Debug)]
pub struct IpDiscoveryPacket {
	// ty: u16,
	// length: u16,
	ssrc: u32,
	ip: Ipv4Addr,
	port: u16,
}

impl IpDiscoveryPacket {
	const SEND_TYPE: u16 = 1;
	const RECV_TYPE: u16 = 2;
	const LENGTH: u16 = 70;

	pub const fn packet_size() -> usize {
		let ty = size_of::<u16>();
		let length = size_of::<u16>();
		let ssrc = size_of::<u32>();
		let ip = size_of::<[u8; 64]>();
		let port = size_of::<u16>();

		ty + length + ssrc + ip + port
	}

	pub fn new_send_ssrc(ssrc: u32) -> Self {
		Self { ssrc, ip: Ipv4Addr::from(0), port: 0 }
	}

	pub fn ssrc(&self) -> u32 {
		self.ssrc
	}

	pub fn ip(&self) -> &Ipv4Addr {
		&self.ip
	}

	pub fn port(&self) -> u16 {
		self.port
	}
}

impl TryFrom<&[u8]> for IpDiscoveryPacket {
	type Error = ();

	fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
		if value.len() != Self::packet_size() {
			return Err(());
		}

		let mut cur = io::Cursor::new(value);
		let ty = cur.read_u16::<NetworkEndian>().map_err(|_| ())?;

		if ty != Self::RECV_TYPE {
			return Err(());
		}

		let length = cur.read_u16::<NetworkEndian>().map_err(|_| ())?;
		if length != Self::LENGTH {
			return Err(());
		}

		let ssrc = cur.read_u32::<NetworkEndian>().map_err(|_| ())?;

		let mut ip = vec![0; 64];
		cur.read_exact(&mut ip).map_err(|_| ())?;
		ip.truncate(ip.iter().position(|&x| x == 0).unwrap_or(ip.len()));
		let ip = String::from_utf8(ip).map_err(|_| ())?;
		let ip = Ipv4Addr::from_str(&ip).map_err(|_| ())?;

		let port = cur.read_u16::<NetworkEndian>().map_err(|_| ())?;

		Ok(Self { ssrc, ip, port })
	}
}

impl From<IpDiscoveryPacket> for Vec<u8> {
	fn from(value: IpDiscoveryPacket) -> Self {
		let mut buf = Vec::with_capacity(IpDiscoveryPacket::packet_size());
		buf.extend_from_slice(&IpDiscoveryPacket::SEND_TYPE.to_be_bytes());
		buf.extend_from_slice(&IpDiscoveryPacket::LENGTH.to_be_bytes());
		buf.extend_from_slice(&value.ssrc.to_be_bytes());
		buf.extend_from_slice(value.ip.to_string().as_bytes());
		buf.resize(IpDiscoveryPacket::packet_size() - 2, 0);
		buf.extend_from_slice(&value.port.to_be_bytes());
		buf
	}
}
