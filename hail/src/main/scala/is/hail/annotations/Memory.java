package is.hail.annotations;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("sunapi")
public class Memory {
    private static final MemoryInterface mi;


    public static void storeByte(byte[] mem, long off, byte b) {
        mi.storeByte(mem, off, b);
    }

    public static void storeInt(byte[] mem, long off, int i) {
        mi.storeInt(mem, off, i);
    }

    public static void storeLong(byte[] mem, long off, long l) {
        mi.storeLong(mem, off, l);
    }

    public static void storeFloat(byte[] mem, long off, float f) {
        mi.storeFloat(mem, off, f);
    }

    public static void storeDouble(byte[] mem, long off, double d) {
        mi.storeDouble(mem, off, d);
    }

    public static void storeAddress(byte[] mem, long off, long a) {
        mi.storeAddress(mem, off, a);
    }

    public static byte loadByte(byte[] mem, long off) {
        return mi.loadByte(mem, off);
    }

    public static float loadFloat(byte[] mem, long off) {
        return mi.loadFloat(mem, off);
    }

    public static int loadInt(byte[] mem, long off) {
        return mi.loadInt(mem, off);
    }

    public static long loadLong(byte[] mem, long off) {
        return mi.loadLong(mem, off);
    }

    public static double loadDouble(byte[] mem, long off) {
        return mi.loadDouble(mem, off);
    }

    public static long loadAddress(byte[] mem, long off) {
        return mi.loadAddress(mem, off);
    }

    public static void memcpy(byte[] dst, long dstOff, byte[] src, long srcOff, long n) {
        mi.memcpy(dst, dstOff, src, srcOff, n);
    }

    // srcOff is in doubles, n is in doubles
    public static void memcpy(byte[] dst, long dstOff, double[] src, long srcOff, long n) {
        mi.memcpy(dst, dstOff, src, srcOff, n);
    }

    // dstOff is in doubles, n is in doubles
    public static void memcpy(double[] dst, long dstOff, byte[] src, long srcOff, long n) {
        mi.memcpy(dst, dstOff, src, srcOff, n);
    }

    public static void memcpy(long dst, byte[] src, long srcOff, long n) {
        mi.memcpy(dst, src, srcOff, n);
    }

    public static void memcpy(byte[] dst, long dstOff, long src, long n) {
        mi.memcpy(dst, dstOff, src, n);
    }

    public static void memset(long offset, long size, byte b) {
        mi.memset(offset, size, b);
    }

    public static boolean loadBoolean(long addr) {
        return mi.loadBoolean(addr);
    }

    public static byte loadByte(long addr) {
        return mi.loadByte(addr);
    }

    public static short loadShort(long addr) {
        return mi.loadShort(addr);
    }

    public static int loadInt(long addr) {
        return mi.loadInt(addr);
    }

    public static long loadLong(long addr) {
        return mi.loadLong(addr);
    }

    public static float loadFloat(long addr) {
        return mi.loadFloat(addr);
    }

    public static double loadDouble(long addr) {
        return mi.loadDouble(addr);
    }

    public static long loadAddress(long addr) {
        return mi.loadAddress(addr);
    }

    public static void storeBoolean(long addr, boolean b) {
        mi.storeBoolean(addr, b);
    }

    public static void storeByte(long addr, byte b) {
        mi.storeByte(addr, b);
    }

    public static void storeShort(long addr, short s) {
        mi.storeShort(addr, s);
    }

    public static void storeInt(long addr, int i) {
        mi.storeInt(addr, i);
    }

    public static void storeLong(long addr, long l) {
        mi.storeLong(addr, l);
    }

    public static void storeFloat(long addr, float f) {
        mi.storeFloat(addr, f);
    }

    public static void storeDouble(long addr, double d) {
        mi.storeDouble(addr, d);
    }

    public static void storeAddress(long addr, long a) {
        mi.storeAddress(addr, a);
    }


    public static long malloc(long size) {
        return mi.malloc(size);
    }

    public static void free(long a) {
       mi.free(a);
    }

    public static void memcpy(long dst, long src, long n) {
        mi.memcpy(dst, src, n);
    }

    public static void copyToArray(byte[] dst, long dstOff, long src, long n) {
        mi.copyToArray(dst, dstOff, src, n);
    }

    public static void copyFromArray(long dst, byte[] src, long srcOff, long n) {
        mi.copyFromArray(dst, src, srcOff, n);
    }

    static {
        mi = new CheckedMemory();
    }
}
