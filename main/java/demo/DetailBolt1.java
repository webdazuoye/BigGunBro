package demo;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DetailBolt1 extends BaseBasicBolt{

    public void execute(Tuple tuple,BasicOutputCollector collector) {
        byte[] bytes=tuple.getBinary(0);  //字节码

        String line =new String(bytes);
        String[] str=line.split(",");
//        String orderdetail_id =str[0];
//        String orderid = str[1];
//        String sender  = str[2];
//        String sendplace = str[3];
        String recipient = str[4].replace("recipient","").replace("\"","").replace(":","");
        String receiveplace = str[5].replace("\"receiveplace\"","").replace("\"","").replace(":","").substring(0,2);
        String methodofpayment = str[6].replace("methodofpayment","").replace("\"","").replace(":","");
//        String paytime= str[7];
//        String freight = str[8];
        String total = str[9].replace("total\":\"","").replace("\"","").replace("}]","");

        collector.emit(new Values(recipient,receiveplace,methodofpayment,total));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("recipient", "receiveplace","methodofpayment","total"));
    }

}
