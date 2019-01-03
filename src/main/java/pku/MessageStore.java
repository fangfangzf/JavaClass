package pku;

import io.Snappy;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class MessageStore {
    static final MessageStore store = new MessageStore();
    HashMap<String, DataOutputStream> outputFiles = new HashMap<>();
    HashMap<String, MappedByteBuffer> inputFiles = new HashMap<>();
    static AtomicInteger pushCount = new AtomicInteger();

    public static byte[] zip(byte[] data) {
        byte[] result = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(data);
            gzip.close();
            result = out.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public static byte[] compress(byte srcBytes[]) throws IOException {
        return Snappy.compress(srcBytes);
    }

    public static byte[] uncompress(byte[] bytes) throws IOException {
        return Snappy.uncompress(bytes, 0, bytes.length);
    }

    public static byte[] unzip(byte[] data) {
        byte[] result = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            GZIPInputStream gzip = new GZIPInputStream(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            byte[] buf = new byte[1024];
            int num = -1;

            while ((num = gzip.read(buf, 0, buf.length)) != -1) {
                out.write(buf, 0, num);
            }
            result = out.toByteArray();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public void push(ByteMessage msg, String topic) throws IOException {
        if (msg == null) {
            return;
        }
        DataOutputStream outTemp;
        synchronized (outputFiles) {
            if (!outputFiles.containsKey(topic)) {
                outTemp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("./data/" + topic, true)));
                outputFiles.put(topic, outTemp);
            } else {
                outTemp = outputFiles.get(topic);
            }
        }
        byte[] body = null;
        byte headNum = (byte) msg.headers().keySet().size();
        byte isZip;
        if (msg.getBody().length > 1024) {
//            body = zip(msg.getBody());
            isZip = 1;
            body = compress(msg.getBody());
        } else {
            body = msg.getBody();
            isZip = 0;
        }
        synchronized (outTemp) {
            outTemp.writeByte(headNum);
            for (String key : msg.headers().keySet()) {
                byte headerOrder = HeaderProcessor.name2order.get(key);
                outTemp.writeByte(headerOrder);
                switch (HeaderProcessor.order2type[headerOrder]) {
                    case 1:
                        outTemp.writeLong(msg.headers().getLong(key));
                        break;
                    case 2:
                        outTemp.writeDouble(msg.headers().getDouble(key));
                        break;
                    case 3:
                        outTemp.writeInt(msg.headers().getInt(key));
                        break;
                    case 4:
                        outTemp.writeUTF(msg.headers().getString(key));
                        break;
                }
            }
            outTemp.writeByte(isZip);
            outTemp.writeShort(body.length);
            outTemp.write(body);
        }
    }

    public ByteMessage pull(String queue, String topic) throws IOException {
        String k = queue + " " + topic;
        MappedByteBuffer inTemp;
        if (!inputFiles.containsKey(k)) {
            try {
                RandomAccessFile fis = new RandomAccessFile(new File("data/" + topic), "rw");
                FileChannel channel = fis.getChannel();
                long size = channel.size();
                inTemp = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            } catch (FileNotFoundException e) {
                return null;
            }
            synchronized (inputFiles) {
                inputFiles.put(k, inTemp);
            }
        } else {
            inTemp = inputFiles.get(k);
        }

        if (inTemp.hasRemaining()) {
            ByteMessage msg = new DefaultMessage();
            byte headerOrder;
            short stringHeaderSize;
            String stringHeader;
            byte[] stringHeaderBytes;
            byte headNum = inTemp.get();
            for (int i = 0; i < headNum; i++) {
                headerOrder = inTemp.get();
                switch (HeaderProcessor.order2type[headerOrder]) {
                    case (short) 1:
                        msg.putHeaders(HeaderProcessor.order2name[headerOrder], inTemp.getLong());
                        break;
                    case (short) 2:
                        msg.putHeaders(HeaderProcessor.order2name[headerOrder], inTemp.getDouble());
                        break;
                    case (short) 3:
                        msg.putHeaders(HeaderProcessor.order2name[headerOrder], inTemp.getInt());
                        break;
                    case (short) 4:
                        stringHeaderSize = inTemp.getShort();
                        stringHeaderBytes = new byte[stringHeaderSize];
                        inTemp.get(stringHeaderBytes);
                        stringHeader = new String(stringHeaderBytes, StandardCharsets.UTF_8);
                        msg.putHeaders(HeaderProcessor.order2name[headerOrder], stringHeader);
                        break;
                }
            }
            byte isZip = inTemp.get();
            short bodySize = inTemp.getShort();
            byte[] body = new byte[bodySize];
            inTemp.get(body);
            if (isZip == 1) {
//                msg.setBody(unzip(body));
                msg.setBody(uncompress(body));
            } else {
                msg.setBody(body);
            }
            return msg;
        } else {
            return null;
        }
    }

    public void increase() {
        pushCount.incrementAndGet();
    }

    public void flush() throws IOException {
        if (pushCount.decrementAndGet() == 0) {
            for (String key : outputFiles.keySet()) {
                outputFiles.get(key).close();
            }
        }
    }
}
