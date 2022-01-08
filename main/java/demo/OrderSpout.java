package demo;

import kafka.api.OffsetRequest;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;

public class OrderSpout {
    public KafkaSpout createKafkSpout(){
        String brokerZkStr = "192.168.205.141:2181,192.168.205.142:2181,192.168.205.143:2181";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);   // 通过zookeeper中的/brokers即可找到kafka的地址
        String topic = "order";
        String zkRoot = "/" + topic;
        String id = "order-id";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);

        spoutConf.startOffsetTime = OffsetRequest.LatestTime(); // 设置之后，刚启动时就不会把之前的消费也进行读取，会从最新的偏移量开始读取
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
        return kafkaSpout;
    }

}
