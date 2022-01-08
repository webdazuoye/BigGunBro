package demo;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class DetailBolt2 extends BaseBasicBolt{

    static JedisPool pool = new JedisPool(new JedisPoolConfig(), "192.168.205.140");
    static Jedis jedis = pool.getResource();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //计算订单数
        jedis.incrBy("order_num",1);
        //System.out.println(order_num);

        //计算销售额
        String total= tuple.getStringByField("total");
        double money = Double.parseDouble(total);
        jedis.incrByFloat("sales",money);
        //System.out.println(sale);

        //计算下单客户数
        String recipient = tuple.getStringByField("recipient");
        jedis.zincrby("recipient",1,recipient);

        //计算支付方式
        String methodofpayment = tuple.getStringByField("methodofpayment");
        jedis.zincrby("methodofpayment",1,methodofpayment);

        //计算地区
        String receiveplace = tuple.getStringByField("receiveplace");
        jedis.zincrby("receiveplace",1,receiveplace);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
