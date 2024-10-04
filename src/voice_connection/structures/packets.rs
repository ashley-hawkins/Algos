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

#[derive(Debug)]
pub struct RtpExtension<'a> {
	id: u16,
	payload: &'a [u8],
}

impl RtpExtension<'_> {
	pub fn id(&self) -> u16 {
		self.id
	}

	pub fn payload(&self) -> &[u8] {
		self.payload
	}

	pub fn len(&self) -> usize {
		4 + self.payload.len() * 4
	}
}

pub trait RtpPacketTrait {
	fn data(&self) -> &[u8];

	fn version(&self) -> u8 {
		self.data()[0] >> 6 & 0b11
	}

	fn padding(&self) -> bool {
		self.data()[0] >> 5 & 0b1 == 1
	}

	fn has_extension(&self) -> bool {
		self.data()[0] >> 4 & 0b1 == 1
	}

	fn extension(&self) -> Option<RtpExtension> {
		let has_extension = self.has_extension();
		if !has_extension {
			return None;
		}

		let fixed_header_length = self.get_fixed_header_length();

		if self.data().len() < fixed_header_length + 4 {
			return None;
		}
		let id = u16::from_be_bytes([
			self.data()[fixed_header_length],
			self.data()[fixed_header_length + 1],
		]);
		let length = u16::from_be_bytes([
			self.data()[fixed_header_length + 2],
			self.data()[fixed_header_length + 3],
		]);

		if self.data().len() < fixed_header_length + 4 + length as usize {
			return None;
		}

		let offset = fixed_header_length + 4;
		Some(RtpExtension { id, payload: &self.data()[offset..(offset + length as usize)] })
	}

	fn csrc_count(&self) -> u8 {
		self.data()[0] & 0b1111
	}

	fn marker(&self) -> bool {
		self.data()[1] >> 7 & 0b1 == 1
	}

	fn payload_type(&self) -> u8 {
		self.data()[1] & 0b1111111
	}

	fn sequence_number(&self) -> u16 {
		u16::from_be_bytes([self.data()[2], self.data()[3]])
	}

	fn timestamp(&self) -> u32 {
		u32::from_be_bytes([self.data()[4], self.data()[5], self.data()[6], self.data()[7]])
	}

	fn ssrc(&self) -> u32 {
		u32::from_be_bytes([self.data()[8], self.data()[9], self.data()[10], self.data()[11]])
	}

	fn csrc(&self, index: u8) -> Result<u32, ()> {
		if index >= self.csrc_count() {
			return Err(());
		}

		let offset = 12 + index as usize * 4;
		Ok(u32::from_be_bytes([
			self.data()[offset],
			self.data()[offset + 1],
			self.data()[offset + 2],
			self.data()[offset + 3],
		]))
	}

	fn get_fixed_header_length(&self) -> usize {
		12 + self.csrc_count() as usize * size_of::<u32>()
	}

	fn get_total_header_length(&self) -> usize {
		self.get_fixed_header_length() + self.extension().map_or(0, |x| x.len())
	}

	fn get_payload(&self) -> &[u8] {
		&self.data()[self.get_fixed_header_length()..]
	}

	fn is_rtcp(&self) -> bool {
		self.payload_type() >= 72 && self.payload_type() <= 76
	}

	fn is_valid(&self) -> bool {
		let raw_data_len = self.data().len();

		if raw_data_len < 12 {
			return false;
		}

		if self.version() != 2 {
			return false;
		}

		if self.is_rtcp() {
			return false;
		}

		if self.has_extension() && self.extension().is_none() {
			return false;
		}

		if raw_data_len < self.get_total_header_length() {
			return false;
		}

		true
	}
}

#[derive(Debug)]
pub struct RtpPacket(Vec<u8>);

impl RtpPacket {
	pub fn new(data: Vec<u8>) -> Self {
		Self(data)
	}

	pub fn into_raw(self) -> Vec<u8> {
		self.0
	}
}

impl RtpPacketTrait for RtpPacket {
	fn data(&self) -> &[u8] {
		&self.0
	}
}

impl TryFrom<&[u8]> for RtpPacket {
	type Error = ();

	fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
		let res = Self(value.to_vec());

		if !res.is_valid() {
			return Err(());
		}

		Ok(res)
	}
}

#[derive(Debug, Clone, Copy)]
pub struct RtpPacketBorrow<'a>(&'a [u8]);

impl<'a> RtpPacketBorrow<'a> {
	pub fn new(data: &'a [u8]) -> Self {
		Self(data)
	}
}

impl<'a> RtpPacketTrait for RtpPacketBorrow<'a> {
	fn data(&self) -> &[u8] {
		self.0
	}
}

impl<'a> TryFrom<&'a [u8]> for RtpPacketBorrow<'a> {
	type Error = ();

	fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
		let res = Self(value);

		if !res.is_valid() {
			return Err(());
		}

		Ok(res)
	}
}
