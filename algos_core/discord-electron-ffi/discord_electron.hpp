#include <cstdint>

enum DiscordFrameType {
  DISCORD_FRAME_NATIVE,
  DISCORD_FRAME_I420,
};

struct DiscordYUVFrame {
  uint8_t const *y;
  uint8_t const *u;
  uint8_t const *v;
  int32_t y_stride;
  int32_t u_stride;
  int32_t v_stride;
};

struct DiscordFrame {
  int64_t timestamp_us;
  union {
    DiscordYUVFrame yuv;
    void* texture_handle;
  } frame;
  int32_t width;
  int32_t height;
  int32_t type;
};

using DiscordFrameReleaseCB = void (*)(void *);

extern "C" {
__attribute__((visibility("default")))
    void DeliverDiscordFrame(const char* streamId,
                             const DiscordFrame& frame,
                             DiscordFrameReleaseCB releaseCB,
                             void* userData);
}
