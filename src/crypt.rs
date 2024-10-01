use crate::connection::structures::{RtpPacket, RtpPacketBorrow, RtpPacketTrait};

use alkali::mem::FullAccess;
use alkali::symmetric::aead;
use alkali::symmetric::cipher as secretbox;
use constants::*;

#[derive(Clone, Copy)]
enum NonceType {
	RtpHeader,
	Lite,
	Suffix,
}

#[derive(Clone, Copy, Debug)]
#[repr(u32)]
pub enum Mode {
	XSalsa20Poly1305,
	XSalsa20Poly1305Suffix,
	XSalsa20Poly1305Lite,
	XSalsa20Poly1305LiteRtpSize,
	AeadAes256Gcm,
	AeadAes256GcmRtpSize,
	AeadXChaCha20Poly1305RtpSize,
	Unknown,
}

impl Mode {
	fn name(self) -> &'static str {
		self.into()
	}

	fn is_rtp_size(self) -> bool {
		matches!(
			self,
			Mode::XSalsa20Poly1305LiteRtpSize
				| Mode::AeadAes256GcmRtpSize
				| Mode::AeadXChaCha20Poly1305RtpSize
		)
	}

	fn nonce_type(self) -> NonceType {
		match self {
			Mode::XSalsa20Poly1305 => NonceType::RtpHeader,
			Mode::XSalsa20Poly1305Suffix => NonceType::Suffix,
			Mode::XSalsa20Poly1305Lite
			| Mode::XSalsa20Poly1305LiteRtpSize
			| Mode::AeadAes256Gcm
			| Mode::AeadAes256GcmRtpSize
			| Mode::AeadXChaCha20Poly1305RtpSize => NonceType::Lite,
			_ => todo!(),
		}
	}
}

impl From<String> for Mode {
	fn from(value: String) -> Self {
		Self::from(value.as_str())
	}
}

impl From<&String> for Mode {
	fn from(value: &String) -> Self {
		Self::from(value.as_str())
	}
}

impl From<&str> for Mode {
	fn from(value: &str) -> Self {
		match value {
			"xsalsa20_poly1305" => Mode::XSalsa20Poly1305,
			"xsalsa20_poly1305_suffix" => Mode::XSalsa20Poly1305Suffix,
			"xsalsa20_poly1305_lite" => Mode::XSalsa20Poly1305Lite,
			"xsalsa20_poly1305_lite_rtpsize" => Mode::XSalsa20Poly1305LiteRtpSize,
			"aead_aes256_gcm" => Mode::AeadAes256Gcm,
			"aead_aes256_gcm_rtpsize" => Mode::AeadAes256GcmRtpSize,
			"aead_xchacha20_poly1305_rtpsize" => Mode::AeadXChaCha20Poly1305RtpSize,
			_ => Mode::Unknown,
		}
	}
}

impl From<Mode> for String {
	fn from(value: Mode) -> Self {
		value.name().to_string()
	}
}

impl From<Mode> for &'static str {
	fn from(value: Mode) -> Self {
		match value {
			Mode::XSalsa20Poly1305 => "xsalsa20_poly1305",
			Mode::XSalsa20Poly1305Suffix => "xsalsa20_poly1305_suffix",
			Mode::XSalsa20Poly1305Lite => "xsalsa20_poly1305_lite",
			Mode::XSalsa20Poly1305LiteRtpSize => "xsalsa20_poly1305_lite_rtpsize",
			Mode::AeadAes256Gcm => "aead_aes256_gcm",
			Mode::AeadAes256GcmRtpSize => "aead_aes256_gcm_rtpsize",
			Mode::AeadXChaCha20Poly1305RtpSize => "aead_xchacha20_poly1305_rtpsize",
			Mode::Unknown => "unknown",
		}
	}
}

pub mod constants {
	pub const KEY_BYTES: usize = 32;
	pub const NONCE_MAX_BYTES: usize = 24;
	pub const NONCE_SUFFIX_BYTES: usize = 24;
	pub const NONCE_LITE_BYTES: usize = 4;
	pub const MAC_BYTES: usize = 16;
}

pub struct VoiceConnectionCrypt {
	next_suffix: u32,
	key: [u8; constants::KEY_BYTES],
	mode: Mode,
}

impl VoiceConnectionCrypt {
	pub fn new() -> Self {
		Self { next_suffix: 0, key: [0; constants::KEY_BYTES], mode: Mode::Unknown }
	}

	pub fn set_key(&mut self, key: &[u8; constants::KEY_BYTES]) {
		self.key.copy_from_slice(key);
	}

	pub fn set_mode(&mut self, mode: Mode) {
		self.mode = mode;
	}

	pub fn get_cleartext_length(mode: Mode, packet: RtpPacketBorrow) -> usize {
		12 + if mode.is_rtp_size() {
			packet.extension().map_or(0, |_| 4) + packet.csrc_count() as usize * 4
		} else {
			0
		}
	}

	pub fn decrypt_in_place(&self, packet: &mut [u8]) -> Option<(usize, usize)> {
		if matches!(self.mode, Mode::Unknown) {
			return None;
		}
		let rtp_packet_view: RtpPacketBorrow = match (&*packet).try_into() {
			Ok(packet) => packet,
			Err(_) => return None,
		};

		let header_length = rtp_packet_view.get_total_header_length();
		let cleartext_length = Self::get_cleartext_length(self.mode, rtp_packet_view);

		let (cleartext, mut ciphertext) = packet.split_at_mut(cleartext_length);
		let nonce: [u8; NONCE_MAX_BYTES] = {
			let mut nonce = [0u8; NONCE_MAX_BYTES];
			match self.mode.nonce_type() {
				NonceType::Lite => {
					let (new_ciphertext, nonce_src) = ciphertext
						.split_last_chunk_mut::<{ constants::NONCE_LITE_BYTES }>()
						.unwrap();
					ciphertext = new_ciphertext;
					nonce[..constants::NONCE_LITE_BYTES].copy_from_slice(nonce_src)
				}
				NonceType::Suffix => {
					let (new_ciphertext, nonce_src) = ciphertext
						.split_last_chunk_mut::<{ constants::NONCE_LITE_BYTES }>()
						.unwrap();
					ciphertext = new_ciphertext;
					nonce[..constants::NONCE_LITE_BYTES].copy_from_slice(nonce_src)
				}
				NonceType::RtpHeader => {
					let len = std::cmp::min(NONCE_MAX_BYTES, header_length);
					nonce[..len].copy_from_slice(&cleartext[..len])
				}
			};
			nonce
		};

		let res = match self.mode {
			Mode::AeadAes256Gcm | Mode::AeadAes256GcmRtpSize => {
				let key: aead::aes256gcm::Key<FullAccess> = (&self.key).try_into().unwrap();

				aead::aes256gcm::decrypt_in_place(
					ciphertext,
					Some(cleartext),
					&key,
					nonce[..aead::aes256gcm::NONCE_LENGTH].try_into().unwrap(),
				)
			}
			Mode::AeadXChaCha20Poly1305RtpSize => {
				let key: aead::xchacha20poly1305_ietf::Key<FullAccess> =
					(&self.key).try_into().unwrap();

				aead::xchacha20poly1305_ietf::decrypt_in_place(
					ciphertext,
					Some(cleartext),
					&key,
					&nonce,
				)
			}
			Mode::XSalsa20Poly1305
			| Mode::XSalsa20Poly1305Suffix
			| Mode::XSalsa20Poly1305Lite
			| Mode::XSalsa20Poly1305LiteRtpSize => {
				let key: secretbox::Key<FullAccess> = (&self.key).try_into().unwrap();
				secretbox::decrypt_in_place(ciphertext, &key, &nonce)
			}
			Mode::Unknown => todo!(),
		};

		res.ok().map(|ct_len| (header_length, cleartext.len() + ct_len))
	}

	// fn encrypt_in_place(&self, buffer: &mut [u8], packet_length: usize) -> Option<&[u8]> {}
}
