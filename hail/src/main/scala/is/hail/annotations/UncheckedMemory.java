package is.hail.annotations;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

@SuppressWarnings("sunapi")
public final class UncheckedMemory implements MemoryInterface {
    private final Unsafe unsafe;

    public void storeByte(byte[] mem, long off, byte b) {
        unsafe.putByte(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, b);
    }

    public void storeInt(byte[] mem, long off, int i) {
        unsafe.putInt(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, i);
    }

    public void storeLong(byte[] mem, long off, long l) {
        unsafe.putLong(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, l);
    }

    public void storeFloat(byte[] mem, long off, float f) {
        unsafe.putFloat(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, f);
    }

    public void storeDouble(byte[] mem, long off, double d) {
        unsafe.putDouble(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, d);
    }

    public void storeAddress(byte[] mem, long off, long a) {
        unsafe.putLong(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, a);
    }

    public byte loadByte(byte[] mem, long off) {
        return unsafe.getByte(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public float loadFloat(byte[] mem, long off) {
        return unsafe.getFloat(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public int loadInt(byte[] mem, long off) {
        return unsafe.getInt(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public long loadLong(byte[] mem, long off) {
        return unsafe.getLong(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public double loadDouble(byte[] mem, long off) {
        return unsafe.getDouble(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public long loadAddress(byte[] mem, long off) {
        // Unsafe has no getAddress on Object
        return unsafe.getLong(mem, Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
    }

    public void memcpy(byte[] dst, long dstOff, byte[] src, long srcOff, long n) {
        if (n > 0) {
            unsafe.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcOff, dst, Unsafe.ARRAY_BYTE_BASE_OFFSET + dstOff, n);
        }
    }

    // srcOff is in doubles, n is in doubles
    public void memcpy(byte[] dst, long dstOff, double[] src, long srcOff, long n) {
        if (n > 0) {
            unsafe.copyMemory(src, Unsafe.ARRAY_DOUBLE_BASE_OFFSET + srcOff * 8, dst, Unsafe.ARRAY_BYTE_BASE_OFFSET + dstOff, n * 8);
        }
    }

    // dstOff is in doubles, n is in doubles
    public void memcpy(double[] dst, long dstOff, byte[] src, long srcOff, long n) {
        if (n > 0) {
            unsafe.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcOff, dst, Unsafe.ARRAY_DOUBLE_BASE_OFFSET + dstOff * 8, n * 8);
        }
    }

    public void memcpy(long dst, byte[] src, long srcOff, long n) {
        copyFromArray(dst, src, srcOff, n);
    }

    public void memcpy(byte[] dst, long dstOff, long src, long n) {
        copyToArray(dst, dstOff, src, n);
    }

    public void memset(long offset, long size, byte b) {
        unsafe.setMemory(offset, size, b);
    }

    public boolean loadBoolean(long addr) {
        return unsafe.getByte(addr) != 0;
    }

    public byte loadByte(long addr) {
        return unsafe.getByte(addr);
    }

    public short loadShort(long addr) {
        return unsafe.getShort(addr);
    }

    public int loadInt(long addr) {
        return unsafe.getInt(addr);
    }

    public long loadLong(long addr) {
        return unsafe.getLong(addr);
    }

    public float loadFloat(long addr) {
        return unsafe.getFloat(addr);
    }

    public double loadDouble(long addr) {
        return unsafe.getDouble(addr);
    }

    public long loadAddress(long addr) {
        return unsafe.getAddress(addr);
    }

    public void storeBoolean(long addr, boolean b) {
        unsafe.putByte(addr, (byte)(b ? 1 : 0));
    }

    public void storeByte(long addr, byte b) {
        unsafe.putByte(addr, b);
    }

    public void storeShort(long addr, short s) {
        unsafe.putShort(addr, s);
    }

    public void storeInt(long addr, int i) {
        unsafe.putInt(addr, i);
    }

    public void storeLong(long addr, long l) {
        unsafe.putLong(addr, l);
    }

    public void storeFloat(long addr, float f) {
        unsafe.putFloat(addr, f);
    }

    public void storeDouble(long addr, double d) {
        unsafe.putDouble(addr, d);
    }

    public void storeAddress(long addr, long a) {
        unsafe.putAddress(addr, a);
    }

    public long malloc(long size) {
        return unsafe.allocateMemory(size);
    }

    public void free(long a) {
        unsafe.freeMemory(a);
    }

    public long realloc(long a, long newSize) {
        return unsafe.reallocateMemory(a, newSize);
    }


    public void memcpy(long dst, long src, long n) {
        if (n > 0) {
            unsafe.copyMemory(src, dst, n);
        }
    }

    public void copyToArray(byte[] dst, long dstOff, long src, long n) {
        if (n > 0) {
            unsafe.copyMemory(null, src, dst, Unsafe.ARRAY_BYTE_BASE_OFFSET + dstOff, n);
        }
    }

    public void copyFromArray(long dst, byte[] src, long srcOff, long n) {
        if (n > 0) {
            unsafe.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcOff, null, dst, n);
        }
    }

    {
        Unsafe t;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            t = (sun.misc.Unsafe) unsafeField.get(null);
        } catch (Throwable cause) {
            t = null;
        }
        unsafe = t;
    }
}
