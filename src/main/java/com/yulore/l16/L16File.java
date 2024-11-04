package com.yulore.l16;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class L16File {
    static public class L16Header {
        public int body_size;
        public int rate;
        public int interval;
        public int channels;
    }
    static public class L16Slice {
        public long from_start;
        public int raw_len;
        public byte[] raw_data;
    }

    public L16Header header;
    public L16Slice[] slices;

    public static L16File loadL16(final DataInputStream dis) throws IOException {
            /*
            typedef struct {
                uint32_t rate;
                int32_t interval;
                int32_t channels;
            } l16_codec_info_t;

            uint8_t version[4];
            uint32_t body_bytes;
            l16_codec_info_t codec_info;
            */
        if (!validL16MFlag(dis)) {
            log.warn("invalid L16, skip");
            return null;
        }
        final L16Header l16hdr = new L16Header();
        l16hdr.body_size = readInt32(dis);
        l16hdr.rate = readInt32(dis);
        l16hdr.interval = readInt32(dis);
        l16hdr.channels = readInt32(dis);
        log.info("l16 info: bodySize:{}" +
                "\n rate:{}" +
                "\n interval:{}" +
                "\n channels:{}",
                l16hdr.body_size,
                l16hdr.rate,
                l16hdr.interval,
                l16hdr.channels);
        if (l16hdr.rate != 8000 && l16hdr.rate != 16000) {
            log.warn("invalid l16 sample rate:{}, no 8k or 16k, skip", l16hdr.rate);
            return null;
        }

        final L16Slice[] slices = readSlices(dis);
        L16File l16file = new L16File();
        l16file.header = l16hdr;
        l16file.slices = slices;
        return l16file;
    }

    public static int calc_body_bytes(L16Slice[] slices) {
        int body_bytes = 0;
        for (L16Slice slice : slices) {
            body_bytes += 8 + 4 + slice.raw_len;
        }
        return body_bytes;
    }

    private static L16Slice[] readSlices(final DataInputStream dis) throws IOException {
        final List<L16Slice> slices = new LinkedList<>();
        try {
            int idx = 0;
            while (true) {
                /*
                switch_time_t _from_start;
                uint32_t _raw_len;
                uint8_t _raw_data[];
                */
                final int from_start_low = readInt32(dis);
                final int from_start_high = readInt32(dis);
                final L16Slice slice = new L16Slice();
                slice.from_start = ((long) from_start_high << 32) | from_start_low;
                slice.raw_len = readInt32(dis);
                slice.raw_data = new byte[slice.raw_len];
                dis.readFully(slice.raw_data);
                log.info("readSlice: {} => size:{}", idx++, slice.raw_len);
                slices.add(slice);
            }
        } catch (EOFException ex) {
            log.warn("readSlices: {}", ex.toString());
        }
        return slices.toArray(new L16Slice[0]);
    }

    private static boolean validL16MFlag(final DataInputStream dis) throws IOException {
        final byte[] flag = new byte[4];
        if (4 != dis.read(flag)) {
            return false;
        }
        return flag[0] == 'L' && flag[1] == '1' && flag[2] == '6' && flag[3] == '1';
    }

    private static int readInt32(DataInputStream dis) throws IOException {
        int i0 = dis.readUnsignedByte();
        int i1 = dis.readUnsignedByte();
        int i2 = dis.readUnsignedByte();
        int i3 = dis.readUnsignedByte();
        return (i3 << 24 | i2 << 16 | i1 << 8 | i0);
    }
}
