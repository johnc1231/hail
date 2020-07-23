package is.hail.annotations;


public interface MemoryInterface {

    void storeByte(byte[] mem, long off, byte b);

    void storeInt(byte[] mem, long off, int i);

    void storeLong(byte[] mem, long off, long l);

    void storeFloat(byte[] mem, long off, float f);

    void storeDouble(byte[] mem, long off, double d);

    void storeAddress(byte[] mem, long off, long a);

    byte loadByte(byte[] mem, long off);

    float loadFloat(byte[] mem, long off);

    int loadInt(byte[] mem, long off);

    long loadLong(byte[] mem, long off);

    double loadDouble(byte[] mem, long off);

    long loadAddress(byte[] mem, long off);

    void memcpy(byte[] dst, long dstOff, byte[] src, long srcOff, long n);

    // srcOff is in doubles, n is in doubles
    void memcpy(byte[] dst, long dstOff, double[] src, long srcOff, long n);

    // dstOff is in doubles, n is in doubles
    void memcpy(double[] dst, long dstOff, byte[] src, long srcOff, long n);

    void memcpy(long dst, byte[] src, long srcOff, long n);

    void memcpy(byte[] dst, long dstOff, long src, long n);

    void memset(long offset, long size, byte b) ;

    boolean loadBoolean(long addr) ;

    byte loadByte(long addr);

    short loadShort(long addr);

    int loadInt(long addr) ;

    long loadLong(long addr);

    float loadFloat(long addr);

    double loadDouble(long addr);

    long loadAddress(long addr);

    void storeBoolean(long addr, boolean b);

    void storeByte(long addr, byte b);

    void storeShort(long addr, short s);

    void storeInt(long addr, int i);

    void storeLong(long addr, long l);

    void storeFloat(long addr, float f) ;

    void storeDouble(long addr, double d);

    void storeAddress(long addr, long a);

    long malloc(long size) ;

    void free(long a);

    void memcpy(long dst, long src, long n) ;

    void copyToArray(byte[] dst, long dstOff, long src, long n);

    void copyFromArray(long dst, byte[] src, long srcOff, long n);

}
