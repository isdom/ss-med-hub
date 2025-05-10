package com.yulore.util;

public class ByteUtil {
    public static int le4bytes2int32(final byte[] bytes) {
        return((bytes[3] & 0xFF) << 24) |
            ((bytes[2] & 0xFF) << 16) |
            ((bytes[1] & 0xFF) << 8)  |
            (bytes[0] & 0xFF);          // 最低有效字节（小端的第一个字节）
    }
}