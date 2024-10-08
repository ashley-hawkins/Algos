use aes_gcm::aead::AeadMutInPlace;
use aes_gcm::Aes256Gcm;
use aes_gcm::KeyInit;
use constants::*;
use discortp::rtp::RtpExtensionPacket;
use discortp::rtp::RtpPacket;
use discortp::Packet;

pub mod constants {
	pub const KEY_BYTES: usize = 32;
	pub const NONCE_MAX_BYTES: usize = 12;
	pub const NONCE_LITE_BYTES: usize = 4;
	pub const MAC_BYTES: usize = 16;

	// Since we're doing in-place encryption, we will need to pass in a buffer that has enough space for the mac and nonce.
	pub const ENCRYPT_REQUIRED_EXTRA_CAPACITY: usize = NONCE_LITE_BYTES + MAC_BYTES;
}

// TODO: Refactor so that the only inherent state is next_suffix.
// I want this because the other state comes from elsewhere in the code whereas next_suffix is managed internally to the struct.
pub struct VoiceConnectionCrypt {
	next_suffix: u32,
	aes_gcm: Option<aes_gcm::Aes256Gcm>,
}

impl VoiceConnectionCrypt {
	pub const MODE: &'static str = "aead_aes256_gcm_rtpsize";

	pub fn new() -> Self {
		Self { next_suffix: 0, aes_gcm: None }
	}

	pub fn set_key(&mut self, key: &[u8; constants::KEY_BYTES]) {
		self.aes_gcm = Some(aes_gcm::Aes256Gcm::new(key.into()));
	}

	pub fn get_cleartext_length(packet: &RtpPacket) -> usize {
		12 + if packet.get_extension() == 1 { 4 } else { 0 } + packet.get_csrc_count() as usize * 4
	}

	pub fn get_total_header_length(packet: &RtpPacket) -> usize {
		12 + if packet.get_extension() == 1 {
			4 + RtpExtensionPacket::new(packet.payload()).unwrap().get_length() as usize * 4
		} else {
			0
		} + packet.get_csrc_count() as usize * 4
	}

	pub fn decrypt_in_place(&mut self, packet: &mut [u8]) -> Option<(usize, usize)> {
		let aes_gcm = match self.aes_gcm.as_mut() {
			Some(x) => x,
			_ => return None,
		};

		let rtp_packet_view = RtpPacket::new(packet)?;

		let header_length = Self::get_total_header_length(&rtp_packet_view);
		let cleartext_length = Self::get_cleartext_length(&rtp_packet_view);

		let (cleartext, mut ciphertext) = packet.split_at_mut(cleartext_length);

		let mut nonce = [0u8; NONCE_MAX_BYTES];
		let (new_ciphertext, nonce_src) =
			ciphertext.split_last_chunk_mut::<{ constants::NONCE_LITE_BYTES }>().unwrap();
		ciphertext = new_ciphertext;
		nonce[..constants::NONCE_LITE_BYTES].copy_from_slice(nonce_src);

		let (mut ciphertext, tag) =
			ciphertext.split_last_chunk_mut::<{ constants::MAC_BYTES }>().unwrap();
		let res = aes_gcm.decrypt_in_place_detached(
			nonce.as_slice().into(),
			&cleartext,
			&mut ciphertext,
			tag.as_slice().into(),
		);

		res.ok().map(|_| (header_length, cleartext.len() + ciphertext.len()))
	}

	// fn encrypt_in_place(&self, buffer: &mut [u8], packet_length: usize) -> Option<&[u8]> {}
}

impl Default for VoiceConnectionCrypt {
	fn default() -> Self {
		Self::new()
	}
}
