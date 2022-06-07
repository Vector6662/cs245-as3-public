package cs245.as3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private HashMap<Long, ArrayList<LogRecord>> recordMap;

	private StorageManager sm;
	private LogManager lm;
	private PriorityQueue<Long> q;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		recordMap = new HashMap<>();
		q = new PriorityQueue<>();
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this.sm = sm;
		this.lm = lm;

		ArrayList<LogRecord> records = new ArrayList<>(); //log中的所有record
		HashSet<Long> committedTxn = new HashSet<>();
		for (int offset = lm.getLogTruncationOffset(); offset < lm.getLogEndOffset(); ) {
			byte[] sizeBytes = lm.readLogRecord(offset, 4);
			int size = ByteBuffer.wrap(sizeBytes).getInt();
			byte[] recordBytes = lm.readLogRecord(offset, size);
			LogRecord logRecord = LogRecord.decode(recordBytes);
			logRecord.setOffset(offset);
			records.add(logRecord);
			// recover committed records
			if (LogRecord.END == logRecord.getState()) {
				committedTxn.add(logRecord.getTxnId());
			}
			offset += logRecord.getSize();
		}

		for (LogRecord record : records) {
			long txnId = record.getTxnId();
			if (!committedTxn.contains(txnId) || LogRecord.OPS != record.getState())
				continue;

			// todo
			long tag = record.getOffset();
			q.add(tag);
			sm.queueWrite(record.getKey(), tag, record.getValues());
			latestValues.put(record.getKey(), new TaggedValue(tag, record.getValues()));
		}



	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		recordMap.put(txID, new ArrayList<>());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		recordMap.get(txID).add(new LogRecord(txID,key,value,LogRecord.OPS));

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		LogRecord commitRecord = new LogRecord(txID, -1, new byte[]{}, LogRecord.END);
		recordMap.get(txID).add(commitRecord);

		HashMap<Long,Long> keyToOffset = new HashMap<>();
		//persist
		for (LogRecord logRecord : recordMap.get(txID)) {
			long offset = logRecord.persist(logRecord, lm);
			keyToOffset.put(logRecord.getKey(), offset);

//			if (LogRecord.OPS == logRecord.getState()) {
//				keyToOffset.put(logRecord.getKey(), offset);
//			}
		}
		//apply
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = keyToOffset.get(x.key);
				q.add(tag);
				sm.queueWrite(x.key, tag, x.value);
				latestValues.put(x.key, new TaggedValue(tag, x.value));
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
		recordMap.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		if (persisted_tag == q.peek()) {
			lm.setLogTruncationOffset((int)persisted_tag);
		}
		q.remove(persisted_tag);
	}
}
