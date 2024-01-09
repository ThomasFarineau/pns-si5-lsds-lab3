package fr.thomasfar;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.concurrent.TimeUnit;

public class Topology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("random-number-spout", new RandomNumberSpout());
        builder.setBolt("odd-number-bolt", new OddNumberBolt()).shuffleGrouping("random-number-spout");
        builder.setBolt("averaging-bolt", new AveragingBolt()).shuffleGrouping("odd-number-bolt");
        builder.setBolt("windowed-averaging-bolt", new WindowedAveragingBolt()
                        .withWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS)))
                .shuffleGrouping("odd-number-bolt");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
