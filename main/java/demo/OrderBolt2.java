package demo;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class OrderBolt2 extends BaseBasicBolt{

    static JedisPool pool = new JedisPool(new JedisPoolConfig(), "192.168.205.140");
    static Jedis jedis = pool.getResource();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try{
            //计算销售的图书总数
            String book_num = tuple.getStringByField("book_num");
            double sofia = Double.valueOf(book_num);   //sofia为一个订单中买出的书的数量
            jedis.incrByFloat("book_total",sofia);
            System.out.println(sofia);

            //计算每一种图书的销量
            String book_name = tuple.getStringByField("book_name");
            jedis.zincrby("book_name",sofia,book_name);

            //计算每种图书类型的销量
            String book_type = tuple.getStringByField("book_type");
            jedis.zincrby("book_type",sofia,book_type);

            //计算每个出版社的销售数量
            String book_press = tuple.getStringByField("book_press");
            jedis.zincrby("book_press",1,book_press);
        }

        catch(Exception e){
            //System.out.println("ERROR");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){}

}
