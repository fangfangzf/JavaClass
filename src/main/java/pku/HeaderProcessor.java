package pku;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HeaderProcessor {
    public static final String[] order2name = {"MessageId", "Topic", "BornTimestamp", "BornHost", "StoreTimestamp"
            , "StoreHost", "StartTime", "StopTime", "Timeout", "Priority", "Reliability", "SearchKey", "ScheduleExpression"
            , "ShardingKey", "ShardingPartition", "TraceId"};

    public static final byte[] order2type = {3, 4, 1, 4, 1, 4, 1, 1, 3, 3, 3, 4, 4, 2, 2, 4};

    public static final HashMap<String, Byte> name2type = new HashMap<String, Byte>() {
        {
            put("MessageId", (byte) 3);
            put("Topic", (byte) 4);
            put("BornTimestamp", (byte) 1);
            put("BornHost", (byte) 4);
            put("StoreTimestamp", (byte) 1);
            put("StoreHost", (byte) 4);
            put("StartTime", (byte) 1);
            put("StopTime", (byte) 1);
            put("Timeout", (byte) 3);
            put("Priority", (byte) 3);
            put("Reliability", (byte) 3);
            put("SearchKey", (byte) 4);
            put("ScheduleExpression", (byte) 4);
            put("ShardingKey", (byte) 2);
            put("ShardingPartition", (byte) 2);
            put("TraceId", (byte) 4);
        }
    };

    public static final HashMap<String, Byte> name2order = new HashMap<String, Byte>() {
        {
            put("MessageId", (byte) 0);
            put("Topic", (byte) 1);
            put("BornTimestamp", (byte) 2);
            put("BornHost", (byte) 3);
            put("StoreTimestamp", (byte) 4);
            put("StoreHost", (byte) 5);
            put("StartTime", (byte) 6);
            put("StopTime", (byte) 7);
            put("Timeout", (byte) 8);
            put("Priority", (byte) 9);
            put("Reliability", (byte) 10);
            put("SearchKey", (byte) 11);
            put("ScheduleExpression", (byte) 12);
            put("ShardingKey", (byte) 13);
            put("ShardingPartition", (byte) 14);
            put("TraceId", (byte) 15);
        }
    };

//    private static short setBit(short num, int i)
//    {
//        return   (short)(num | (1 << i));
//    }
//
//    private static boolean getBit(short num, int i)
//    {
//        return ((num & (1 << i)) != 0);//true 表示第i位为1,否则为0
//    }
//
//    public static void headerWriter(DataOutputStream temp,KeyValue header){
//        Map<String, Object> map = header.getMap();
//        Set<String> keys = map.keySet();
//        short a = 0;
//        for(int i=0;i<16;i++){
//            if(keys.contains(order[i])) {
//                a = setBit(a,i);
//            }
//        }
//
//        try {
//            temp.writeShort(a);
//            for(int i =0;i<16;i++){
//                if(getBit(a,i)){
//                    switch (order2type[i]){
//                        case 1:
//                            temp.writeLong(header.getLong(order[i]));
//                            break;
//                        case 2:
//                            temp.writeDouble(header.getDouble(order[i]));
//                            break;
//                        case 3:
//                            temp.writeInt(header.getInt(order[i]));
//                            break;
//                        case 4:
//                            temp.writeUTF(header.getString(order[i]));
//                            break;
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static KeyValue headerReader(DataInputStream temp){
//        DefaultKeyValue header = new DefaultKeyValue();
//        try {
//            short sign = temp.readShort();
//            for(int i =0;i<16;i++){
//                if(getBit(sign,i)){
//                    switch (order2type[i]){
//                        case 1:
//                            header.put(order[i],temp.readLong());
//                            break;
//                        case 2:
//                            header.put(order[i],temp.readDouble());
//                            break;
//                        case 3:
//                            header.put(order[i],temp.readInt());
//                            break;
//                        case 4:
//                            header.put(order[i],temp.readUTF());
//                            break;
//                    }
//                }
//            }
//        }catch (IOException e) {
//            e.printStackTrace();
//        }
//        return header;
//    }
}
