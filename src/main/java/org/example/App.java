package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new TweetSpout());
        Config conf = new Config();

        builder.setBolt("DecomposeBolt",new DecomposedBolt(),2).shuffleGrouping("spout");
        builder.setBolt("write_MO",new write_mood_origineBolt(),2).shuffleGrouping("DecomposeBolt");
        builder.setBolt("Trash",new TrashBolt()).shuffleGrouping("DecomposeBolt");
        builder.setBolt("CensorShipBolt",new CensorshipBolt(),2).shuffleGrouping("DecomposeBolt");
        conf.setDebug(true);
        StormTopology topology = builder.createTopology();

        String topologyName = "twitter";
        if (args.length >0 && args[0].equals("remote")){
            StormSubmitter.submitTopology(topologyName,conf,topology);
        } else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName,conf,topology);
        }
    }
}
