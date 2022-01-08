package demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
public class Topology {

    public static void main(String[] args)
            throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {

        Config config = new Config();   // Config()对象继承自HashMap，但本身封装了一些基本的配置
        config.setNumWorkers(4);  //设置worker进程并发度，默认为1
        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout("OrderSpout", new OrderSpout().createKafkSpout());
        builder.setBolt("OrderBolt1", new OrderBolt1()).shuffleGrouping("OrderSpout");
        builder.setBolt("OrderBolt2", new OrderBolt2()).shuffleGrouping("OrderBolt1");

        builder.setSpout("DetailSpout", new DetailSpout().createKafkSpout());
        builder.setBolt("DetailBolt1", new DetailBolt1()).shuffleGrouping("DetailSpout");
        builder.setBolt("DetailBolt2", new DetailBolt2()).shuffleGrouping("DetailBolt1");

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Sofia", config, builder.createTopology());
        }

    }
}
