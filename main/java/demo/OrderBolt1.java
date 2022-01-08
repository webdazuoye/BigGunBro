package demo;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class OrderBolt1 extends BaseBasicBolt{

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        byte[]bytes=tuple.getBinary(0);  //字节码
        //byte->String

        String line =new String(bytes);
        String[] str=line.split("\":\"");

        String book_name = str[3].replace("book_type", "").replace("\"", "").replace(",", "");
        String book_type =str[4].replace("book_num", "").replace("\"", "").replace(",", "");
        String book_num= str[5].replace("book_price", "").replace("\"", "").replace(",", "");
        String book_press=str[7].replace("id", "").replace("\"", "").replace(",", "");


        collector.emit(new Values(book_name,book_type,book_num,book_press));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("book_name","book_type","book_num","book_press"));
    }

}
