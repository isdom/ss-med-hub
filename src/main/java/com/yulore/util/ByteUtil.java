package com.yulore.util;

public class ByteUtil {
    public static int _4bytes2int32AsLE(final byte[] bytes, final int offset) {
        return((bytes[offset+3] & 0xFF) << 24) |
            ((bytes[offset+2] & 0xFF) << 16) |
            ((bytes[offset+1] & 0xFF) << 8)  |
            (bytes[offset] & 0xFF);          // 最低有效字节（小端的第一个字节）
    }

    public static long _8bytes2longAsLE(final byte[] bytes, final int offset) {
        return ((bytes[offset+7] & 0xFFL) << 56) |  // 最高有效字节（小端的最后一个字节）
                ((bytes[offset+5] & 0xFFL) << 40) |
                ((bytes[offset+6] & 0xFFL) << 48) |
                ((bytes[offset+4] & 0xFFL) << 32) |
                ((bytes[offset+3] & 0xFFL) << 24) |
                ((bytes[offset+2] & 0xFFL) << 16) |
                ((bytes[offset+1] & 0xFFL) << 8)  |
                (bytes[offset] & 0xFFL);          // 最低有效字节（小端的第一个字节）
    }
}