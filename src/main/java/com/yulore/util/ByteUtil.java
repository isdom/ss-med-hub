package com.yulore.util;

public class ByteUtil {
    public static int le4bytes2int32(final byte[] bytes) {
        return((bytes[3] & 0xFF) << 24) |
            ((bytes[2] & 0xFF) << 16) |
            ((bytes[1] & 0xFF) << 8)  |
            (bytes[0] & 0xFF);          // 最低有效字节（小端的第一个字节）
    }

    public static long le8bytes2long(final byte[] bytes) {
        return ((bytes[7] & 0xFFL) << 56) |  // 最高有效字节（小端的最后一个字节）
                ((bytes[5] & 0xFFL) << 40) |
                ((bytes[6] & 0xFFL) << 48) |
                ((bytes[4] & 0xFFL) << 32) |
                ((bytes[3] & 0xFFL) << 24) |
                ((bytes[2] & 0xFFL) << 16) |
                ((bytes[1] & 0xFFL) << 8)  |
                (bytes[0] & 0xFFL);          // 最低有效字节（小端的第一个字节）
    }
}