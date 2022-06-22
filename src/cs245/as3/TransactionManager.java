package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.driver.LogManagerImpl;
import cs245.as3.driver.StorageManagerImpl;

import cs245.as3.interfaces.StorageManager.TaggedValue;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;

public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}

	private HashMap<Long, TaggedValue> latestValues;

	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private LogManager lm;
	private StorageManager sm;

	private long MaxTxID;

	private HashSet<Integer> OffsetSet;

	private PriorityQueue<Integer> persisted;

	private Map<Long,ArrayList<Record>> RecordMap;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		OffsetSet=new HashSet<>();
		persisted=new PriorityQueue<>();
		RecordMap=new HashMap<>();
		MaxTxID=-1;
	}
	public void initAndRecover(StorageManager sm, LogManager lm) {

		latestValues = sm.readStoredTable();
		this.sm=sm;
		this.lm=lm;

		ArrayList<Record> Records = new ArrayList<>();

		ArrayList<Integer> tags = new ArrayList<>();

		Set<Long> txCommit = new HashSet<>();

		int logOffset=lm.getLogTruncationOffset();


		while (logOffset < lm.getLogEndOffset()) {


			byte[] bytes = lm.readLogRecord(logOffset, Integer.BYTES);
			ByteBuffer bb = ByteBuffer.wrap(bytes);

			int len=bb.getInt();
			byte[] R_bytes = lm.readLogRecord(logOffset, len);

			Record record = Record.deserialize(R_bytes);

			Records.add(record);
			logOffset+=record.len;
			tags.add(logOffset);


			if (record.type==1){
				txCommit.add(record.txID);
			}
		}


		for (int i = 0; i < Records.size(); i++) {
			Record record = Records.get(i);
			if (txCommit.contains(record.txID)&&record.type==0){
				Integer tag = tags.get(i);
				latestValues.put(record.key, new TaggedValue(tag,record.value));
				sm.queueWrite(record.key, tag,record.value);
				persisted.add(tag);
			}
		}

	}


	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this

		if (txID>MaxTxID){
			MaxTxID=txID;
		}else try {
			throw new Exception("txd is not allow");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}


	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));

		//记录写操作日志到RecordMap中
		ArrayList<Record> records = RecordMap.get(txID);
		if (records == null) {
			records = new ArrayList<>();
			RecordMap.put(txID, records);
		}
		records.add(new Record((byte)0,txID,key,value));
	}

	public void commit(long txID) {

		ArrayList<Record> records = RecordMap.get(txID);
		if (records==null){
			return;
		}
		// 把事务txID的提交(commit=1)Record写到records中
		records.add(new Record(txID, (byte) 1));

		//记录key和对应写入在lm中的尾巴最大偏移量tag
		Map<Long,Integer> keyToTag=new HashMap<>();
		//写入LogManager
		for (Record record : records) {
			lm.appendLogRecord(record.serialize());
			int tag = lm.getLogEndOffset();
			if (record.type==0){
				keyToTag.put(record.key,tag);
			}
		}

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//获取在日志中的偏移量tag
				Integer tag = keyToTag.get(x.key);
				//更新latestValues
				latestValues.put(x.key, new TaggedValue(tag, x.value));
				//写入StorageManager
				sm.queueWrite(x.key,tag,x.value);
				persisted.add(tag);
			}
			writesets.remove(txID);
		}
	}
	public void abort(long txID) {
		RecordMap.remove(txID);
		writesets.remove(txID);
	}
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		OffsetSet.add((int)persisted_tag);
		while (!persisted.isEmpty()&&OffsetSet.contains(persisted.peek())){
			long tag = persisted.poll();
			OffsetSet.remove((int)tag);
			lm.setLogTruncationOffset((int) tag);
		}
	}
}
