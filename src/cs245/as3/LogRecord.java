package cs245.as3;

import cs245.as3.interfaces.LogManager;

import java.nio.ByteBuffer;

public class LogRecord {
    private int size;//4 Bytes
    private long txnId;//8 Bytes
    private long key;//8 Bytes
    private int offset;
    private int state;//4 Bytes
    private byte[] values;//

    // state types
    public static int OPS = 0; //write
    public static int COMMIT = 1; // commit record

    public LogRecord(int size, long tnxId, long key,int state,byte[] values) {
        this.size = size;
        this.txnId = tnxId;
        this.key = key;
        this.state = state;
        this.values = values;
    }

    public LogRecord(long txnId, long key, byte[] values, int state) {
        this.txnId = txnId;
        this.key = key;
        this.state = state;
        this.values = values;
    }


    public static byte[] encode(LogRecord l) {
        int size = 24+l.values.length; // OPS
        l.setSize(size);
        ByteBuffer allocate = ByteBuffer.allocate(size);
        allocate.putInt(l.size)
                .putLong(l.txnId)
                .putLong(l.key)
                .putInt(l.state)
                .put(l.values);
        return allocate.array();
    }

    public static LogRecord decode(byte[] bytes) {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        int size = wrap.getInt();
        long tnxId = wrap.getLong();
        long key = wrap.getLong();
        int state = wrap.getInt();
        byte[] values = new byte[size- wrap.position()];
        wrap.get(values);
        return new LogRecord(size, tnxId, key, state, values);
    }

    public long persist(LogRecord logRecord, LogManager lm){
        byte[] encode = encode(logRecord);
        return lm.appendLogRecord(encode);
    }


    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public byte[] getValues() {
        return values;
    }

    public void setValues(byte[] values) {
        this.values = values;
    }
}
