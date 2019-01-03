package pku;

public class Producer {
    public String topic;

    protected Producer() {
        MessageStore.store.increase();
    }

    //生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body) throws Exception {
        ByteMessage msg = new DefaultMessage(body);
        this.topic = topic;
        return msg;
    }

    //将message发送出去
    public void send(ByteMessage defaultMessage) throws Exception {
        MessageStore.store.push(defaultMessage, topic);
    }

    //处理将缓存区的剩余部分
    public void flush() throws Exception {
        MessageStore.store.flush();
    }
}
