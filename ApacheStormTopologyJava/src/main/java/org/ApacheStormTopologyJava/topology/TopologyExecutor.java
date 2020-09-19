package org.ApacheStormTopologyJava.topology;

import org.ApacheStormTopologyJava.bolt.AggregatingMessagesBolt;
import org.ApacheStormTopologyJava.bolt.FilteringMessageBolt;
import org.ApacheStormTopologyJava.bolt.PrintingAggregatedMessagesBolt;
import org.ApacheStormTopologyJava.spout.RandomMessageSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class TopologyExecutor {
    public static void main(String[] args) throws Exception {
        runTopology();
    }

    public static void runTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomMessageSpout", new RandomMessageSpout());
        builder.setBolt("filteringMessageBolt", new FilteringMessageBolt())
                .shuffleGrouping("randomMessageSpout");
        builder.setBolt("aggregatingMessagesBolt", new AggregatingMessagesBolt()
                .withTimestampField("timestamp")
                .withLag(BaseWindowedBolt.Duration.seconds(1))
                .withWindow(BaseWindowedBolt.Duration.seconds(3)))
                .shuffleGrouping("filteringMessageBolt");
        builder.setBolt("printingAggregatedMessagesBolt", new PrintingAggregatedMessagesBolt())
                .shuffleGrouping("aggregatingMessagesBolt");

        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("ApacheStormTopologyJavaExample", config, builder.createTopology());
    }
}
