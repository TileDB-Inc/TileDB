// stripped to point of non-useability to demonstrate linkage success
// based on cwebp.cc from
// https://chromium.googlesource.com/webm/libwebp
// commit a19a25bb03757d5bb14f8d9755ab39f06d0ae5ef
// Copyright 2011 Google Inc. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style license
// that can be found in the COPYING file in the root of the source
// tree. An additional intellectual property rights grant can be found
// in the file PATENTS. All contributing project authors may
// be found in the AUTHORS file in the root of the source tree.
// -----------------------------------------------------------------------------
//
//  simple command line calling the WebPEncode function.
//  Encodes a raw .YUV into WebP bitstream
//
// Author: Skal (pascal.massimino@gmail.com)

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "webp/decode.h"
#include "webp/encode.h"
#include "webp/mux_types.h"
#include "webp/types.h"

//------------------------------------------------------------------------------

static void AllocExtraInfo(WebPPicture* const pic) {
  const int mb_w = (pic->width + 15) / 16;
  const int mb_h = (pic->height + 15) / 16;
  pic->extra_info =
      (uint8_t*)WebPMalloc(mb_w * mb_h * sizeof(*pic->extra_info));
}

// -----------------------------------------------------------------------------
// Metadata writing.

enum {
  METADATA_EXIF = (1 << 0),
  METADATA_ICC = (1 << 1),
  METADATA_XMP = (1 << 2),
  METADATA_ALL = METADATA_EXIF | METADATA_ICC | METADATA_XMP
};

//------------------------------------------------------------------------------

static int ProgressReport(int percent, const WebPPicture* const picture) {
  fprintf(stderr, "[%s]: %3d %%      \r", (char*)picture->user_data, percent);
  return 1;  // all ok
}

//------------------------------------------------------------------------------

static void HelpShort(void) {
  printf("Usage:\n\n");
  printf("   cwebp [options] -q quality input.png -o output.webp\n\n");
  printf("where quality is between 0 (poor) to 100 (very good).\n");
  printf("Typical value is around 80.\n\n");
  printf("Try -longhelp for an exhaustive list of advanced options.\n");
}

//------------------------------------------------------------------------------
// Error messages

static const char* const kErrorMessages[VP8_ENC_ERROR_LAST] = {
    "OK",
    "OUT_OF_MEMORY: Out of memory allocating objects",
    "BITSTREAM_OUT_OF_MEMORY: Out of memory re-allocating byte buffer",
    "NULL_PARAMETER: NULL parameter passed to function",
    "INVALID_CONFIGURATION: configuration is invalid",
    "BAD_DIMENSION: Bad picture dimension. Maximum width and height "
    "allowed is 16383 pixels.",
    "PARTITION0_OVERFLOW: Partition #0 is too big to fit 512k.\n"
    "To reduce the size of this partition, try using less segments "
    "with the -segments option, and eventually reduce the number of "
    "header bits using -partition_limit. More details are available "
    "in the manual (`man cwebp`)",
    "PARTITION_OVERFLOW: Partition is too big to fit 16M",
    "BAD_WRITE: Picture writer returned an I/O error",
    "FILE_TOO_BIG: File would be too big to fit in 4G",
    "USER_ABORT: encoding abort requested by user"};

//------------------------------------------------------------------------------

uint64_t reference_a_value(uint64_t v) {
  return v + 1;
}

#ifdef _MSC_VER
// do outside of function, because...
//"Use of the warning pragma in the function to change the state of a warning
// number larger than 4699 only takes effect after the end of the function."
#pragma warning(disable : 4701)  // 'config', 'picture_no_alpha'
#endif

extern "C" int WebPGetDemuxVersion(void);
extern "C" int WebPGetMuxVersion(void);
extern "C" VP8StatusCode WebPAllocateDecBuffer(
    int width,
    int height,
    const WebPDecoderOptions* const options,
    WebPDecBuffer* const buffer);
extern "C" void WebPFreeDecBuffer(WebPDecBuffer* const buffer);
typedef void VP8LDecoder;
extern "C" VP8LDecoder* VP8LNew(void);
extern "C" void VP8LClear(VP8LDecoder*);

int main(int, const char*[]) {
  int return_value = -1;
  const char *in_file = NULL, *out_file = NULL;
  FILE* out = NULL;
  int short_output = 0;
  int quiet = 0;
  int blend_alpha = 0;
  uint32_t background_color = 0xffffffu;
  int crop = 0, crop_x = 0, crop_y = 0, crop_w = 0, crop_h = 0;
  int resize_w = 0, resize_h = 0;
  int lossless_preset = 6;
  int use_lossless_preset = -1;  // -1=unset, 0=don't use, 1=use it
  int show_progress = 0;
  int keep_metadata = 0;
  WebPPicture picture;
  int print_distortion = -1;     // -1=off, 0=PSNR, 1=SSIM, 2=LSIM
  WebPPicture original_picture;  // when PSNR or SSIM is requested
  WebPConfig config;
  WebPAuxStats stats;
  WebPMemoryWriter memory_writer;
  int use_memory_writer;
  WebPMemoryWriterInit(&memory_writer);
  if (!WebPPictureInit(&picture) || !WebPPictureInit(&original_picture) ||
      !WebPConfigInit(&config)) {
    fprintf(stderr, "Error! Version mismatch!\n");
  }
  {  // webpdecoder
    // *** turns out, there is -NO- symbol in webpdecoder that is not also in
    // webp! left here anyway, in case libraries are ever de-dupped.
    const int dec_version = WebPGetDecoderVersion();
    auto vp8l_dec = VP8LNew();
    VP8LClear(vp8l_dec);

    VP8StatusCode status;
    WebPDecoderOptions webpdecoptions;
    WebPDecBuffer webpdec_buffer;
    status = WebPAllocateDecBuffer(512, 512, &webpdecoptions, &webpdec_buffer);
    if (status == VP8_STATUS_OK) {
      WebPFreeDecBuffer(&webpdec_buffer);
    }

    // webpdemux
    const int demux_version = WebPGetDemuxVersion();
    // webpmux
    const int mux_version = WebPGetMuxVersion();

    // webp
    // in testing, even when WebP::webp was -not- added as link item, this
    // library was still being placed in link_webp_test project.
    auto enc_version = WebPGetEncoderVersion();

    reference_a_value(dec_version + demux_version + mux_version + enc_version);
  }

  if (argc == 1) {
    HelpShort();
    return 0;
  }
  // add some non-determinism re in_file so compiler can't, seeing that
  // in_file init'd to NULL with no chance of change, avoid generating
  // code (vs2019 was not including any of that code)
  // following the if (in_file == NULL) check below, tho if
  // anyone calls program with argument program will likely fail on
  // that input in its current state...
  else {
    in_file = argv[1];
  }

  if (in_file == NULL) {
    fprintf(stderr, "No input file specified!\n");
    HelpShort();
    // Exercising this stripped executable in build, don't report error
    return 0;
    // goto Error;
  }

  if (use_lossless_preset == 1) {
    if (!WebPConfigLosslessPreset(&config, lossless_preset)) {
      fprintf(stderr, "Invalid lossless preset (-z %d)\n", lossless_preset);
      goto Error;
    }
  }

  // Check for unsupported command line options for lossless mode and log
  // warning for such options.
  if (!quiet && config.lossless == 1) {
    if (config.target_size > 0 || config.target_PSNR > 0) {
      fprintf(
          stderr,
          "Encoding for specified size or PSNR is not supported"
          " for lossless encoding. Ignoring such option(s)!\n");
    }
    if (config.partition_limit > 0) {
      fprintf(
          stderr,
          "Partition limit option is not required for lossless"
          " encoding. Ignoring this option!\n");
    }
  }
  // If a target size or PSNR was given, but somehow the -pass option was
  // omitted, force a reasonable value.
  if (config.target_size > 0 || config.target_PSNR > 0) {
    if (config.pass == 1)
      config.pass = 6;
  }

  if (!WebPValidateConfig(&config)) {
    fprintf(stderr, "Error! Invalid configuration.\n");
    goto Error;
  }

  // Read the input. We need to decide if we prefer ARGB or YUVA
  // samples, depending on the expected compression mode (this saves
  // some conversion steps).
  picture.use_argb =
      (config.lossless || config.use_sharp_yuv || config.preprocessing > 0 ||
       crop || (resize_w | resize_h) > 0);
  picture.progress_hook = (show_progress && !quiet) ? ProgressReport : NULL;

  if (blend_alpha) {
    WebPBlendAlpha(&picture, background_color);
  }

  // The bitstream should be kept in memory when metadata must be appended
  // before writing it to a file/stream, and/or when the near-losslessly encoded
  // bitstream must be decoded for distortion computation (lossy will modify the
  // 'picture' but not the lossless pipeline).
  // Otherwise directly write the bitstream to a file.
  use_memory_writer = (out_file != NULL && keep_metadata) ||
                      (!quiet && print_distortion >= 0 && config.lossless &&
                       config.near_lossless < 100);

  {
    out = NULL;
    if (use_memory_writer) {
      picture.writer = WebPMemoryWrite;
      picture.custom_ptr = (void*)&memory_writer;
    }
    if (!quiet && !short_output) {
      fprintf(stderr, "No output file specified (no -o flag). Encoding will\n");
      fprintf(stderr, "be performed, but its results discarded.\n\n");
    }
  }
  if (!quiet) {
    picture.stats = &stats;
    picture.user_data = (void*)in_file;
  }

  // Crop & resize.
  if (crop != 0) {
    // We use self-cropping using a view.
    if (!WebPPictureView(&picture, crop_x, crop_y, crop_w, crop_h, &picture)) {
      fprintf(stderr, "Error! Cannot crop picture\n");
      goto Error;
    }
  }
  if ((resize_w | resize_h) > 0) {
    WebPPicture picture_no_alpha;
    if (config.exact) {
      // If -exact, we can't premultiply RGB by A otherwise RGB is lost if A=0.
      // We rescale an opaque copy and assemble scaled A and non-premultiplied
      // RGB channels. This is slower but it's a very uncommon use case. Color
      // leak at sharp alpha edges is possible.
      if (!WebPPictureCopy(&picture, &picture_no_alpha)) {
        fprintf(stderr, "Error! Cannot copy temporary picture\n");
        goto Error;
      }

      // We enforced picture.use_argb = 1 above. Now, remove the alpha values.
      {
        int x, y;
        uint32_t* argb_no_alpha = picture_no_alpha.argb;
        for (y = 0; y < picture_no_alpha.height; ++y) {
          for (x = 0; x < picture_no_alpha.width; ++x) {
            argb_no_alpha[x] |= 0xff000000;  // Opaque copy.
          }
          argb_no_alpha += picture_no_alpha.argb_stride;
        }
      }

      if (!WebPPictureRescale(&picture_no_alpha, resize_w, resize_h)) {
        fprintf(stderr, "Error! Cannot resize temporary picture\n");
        goto Error;
      }
    }

    if (!WebPPictureRescale(&picture, resize_w, resize_h)) {
      fprintf(stderr, "Error! Cannot resize picture\n");
      goto Error;
    }

    if (config.exact) {  // Put back the alpha information.
      int x, y;
      uint32_t* argb_no_alpha = picture_no_alpha.argb;
      uint32_t* argb = picture.argb;
      for (y = 0; y < picture_no_alpha.height; ++y) {
        for (x = 0; x < picture_no_alpha.width; ++x) {
          argb[x] = (argb[x] & 0xff000000) | (argb_no_alpha[x] & 0x00ffffff);
        }
        argb_no_alpha += picture_no_alpha.argb_stride;
        argb += picture.argb_stride;
      }
      WebPPictureFree(&picture_no_alpha);
    }
  }

  if (picture.extra_info_type > 0) {
    AllocExtraInfo(&picture);
  }
  // Save original picture for later comparison. Only for lossy as lossless does
  // not modify 'picture' (even near-lossless).
  if (print_distortion >= 0 && !config.lossless &&
      !WebPPictureCopy(&picture, &original_picture)) {
    fprintf(stderr, "Error! Cannot copy temporary picture\n");
    goto Error;
  }

  // Compress.
  if (!WebPEncode(&config, &picture)) {
    fprintf(stderr, "Error! Cannot encode picture as WebP\n");
    fprintf(
        stderr,
        "Error code: %d (%s)\n",
        picture.error_code,
        kErrorMessages[picture.error_code]);
    goto Error;
  }

  // Get the decompressed image for the lossless pipeline.
  if (!quiet && print_distortion >= 0 && config.lossless) {
    if (config.near_lossless == 100) {
      // Pure lossless: image was not modified, make 'original_picture' a view
      // of 'picture' by copying all members except the freeable pointers.
      original_picture = picture;
      original_picture.memory_ = original_picture.memory_argb_ = NULL;
    } else {
      // Decode the bitstream stored in 'memory_writer' to get the altered image
      // to 'picture'; save the 'original_picture' beforehand.
      assert(use_memory_writer);
      original_picture = picture;
      if (!WebPPictureInit(&picture)) {  // Do not free 'picture'.
        fprintf(stderr, "Error! Version mismatch!\n");
        goto Error;
      }

      picture.use_argb = 1;
      picture.stats = original_picture.stats;
    }
    original_picture.stats = NULL;
  }

  return_value = 0;

Error:
  WebPMemoryWriterClear(&memory_writer);
  WebPFree(picture.extra_info);
  WebPPictureFree(&picture);
  WebPPictureFree(&original_picture);
  if (out != NULL && out != stdout) {
    fclose(out);
  }
  return return_value;
}

//------------------------------------------------------------------------------
