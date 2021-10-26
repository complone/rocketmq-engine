package io.openmessaging.engine.util;

import java.nio.ByteBuffer;

/**
 * 尽量采取位运算的方式 避免申请空间消耗
 */
public class TypeTransFrom {

//    public static int byteArrayToInt(byte[] b) {
//        return b[3] & 0xFF |
//                (b[2] & 0xFF << 8) |
//                (b[1] & 0xFF) <<16 |
//                (b[0] & 0xFF ) <<24;
//    }

    public static byte[] intToByteArray(int a){
        return new byte[]{
                (byte) (a & 0xFF) ,
                        (byte) ((a >> 8) & 0xFF) ,
                        (byte) ((a >> 16) & 0xFF) ,
                        (byte) ((a >> 24) & 0xFF)
        };
    }

    private static ByteBuffer buffer = ByteBuffer.allocate(8);

    public static byte[] longToBytes(long x){
        buffer.putLong(0,x);
        return buffer.array();
    }

//    public static long bytesToLong(byte[] bytes){
//        buffer.put(bytes,0,bytes.length);
//        buffer.flip();
//        return buffer.getLong();
//    }

}
