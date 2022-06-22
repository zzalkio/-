package cs245.as3;

import java.nio.ByteBuffer;

public class Record {
    public long txID;   //事务ID
    public long key;    //修改的key
    public byte[] value;//此次key的新值
    public int len;     //该条日志的长度
    public byte type;   //write=0,commit=1;0表示日志为写操作，1表示日志为提交

    public Record(byte type,long txID, long key, byte[] value) {
        this.txID = txID;
        this.key = key;
        this.value = value;
        this.type=type;
        //日志的长度可以通过计算获取
        len=2 * Long.BYTES+Integer.BYTES + value.length+1;
    }

    public Record(int len,byte type,long txID, long key, byte[] value) {
        this.txID = txID;
        this.key = key;
        this.value = value;
        this.len=len;
        this.type=type;
    }

    public Record(long txID, byte type) {
        this.txID = txID;
        this.type = type;
        //日志的长度可以通过计算获取
        len=Long.BYTES+1+Integer.BYTES;
    }

    /**
     * desc 对Record对象的属性值进行转换成byte[]
     * @return byte[]类型
     */
    public byte[] serialize(){
        //申请空间
        ByteBuffer buffer = ByteBuffer.allocate(len);
        //封装存放数据
        //看长度可知该条record日志属于写操作类型还是提交操作类型
        //从而进行不同的封装
        if (len==Long.BYTES+1+Integer.BYTES){
            buffer.putInt(len);
            buffer.putLong(txID);
            buffer.put(type);
            //返回结果
            return buffer.array();
        }else {
            buffer.putInt(len);
            buffer.putLong(txID);
            buffer.put(type);
            buffer.putLong(key);
            buffer.put(value);
            //返回结果
            return buffer.array();
        }

    }

    /**
     * 对传入byte[]数组解析成Record对象
     * @param b 传入的byte数组
     * @return 返回对象Record
     */
    static Record deserialize(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b);
        //看长度可知该条record日志属于写操作类型还是提交操作类型
        //从而进行不同的解析
        if (b.length==Long.BYTES+1+Integer.BYTES){
            int len=bb.getInt();
            long txID = bb.getLong();
            byte type=bb.get();
            return new Record(txID,type);
        }else {
            int len=bb.getInt();
            long txID = bb.getLong();
            byte type=bb.get();
            long key = bb.getLong();
            byte[] value = new byte[b.length - 2 * Long.BYTES-Integer.BYTES-1];
            bb.get(value);
            return new Record(len,type,txID,key,value);
        }

    }
}
