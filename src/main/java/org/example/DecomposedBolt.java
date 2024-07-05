package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class DecomposedBolt extends BaseRichBolt {

    private OutputCollector collector;
    private final String[] mood_list={"sentiment","positive","neutral","negative"};
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("id","dateCreated","source","contains","mood"));
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String[] parts = line.split(",");
        String jsp = parts[0];
        System.out.println("part 0:"+jsp);
        String id = parts[1];
        System.out.println("part 1:"+id);
        String dateCreated = parts[2];
        String source = parts[4];
        String contains = parts[5];
        String mood = parts[6];
        int i = 6;
        boolean ok = false;
        while(!ok){
            for (String m : mood_list) {
                if(m.equals(mood)) {
                    ok = true;
                    break;
                }
            }
            if(!ok){
                contains += mood;
                mood = parts[++i];
            }
        }
        collector.emit(new Values(id,dateCreated,source,contains,mood));
    }
}

// parts[0] = id
// parts[1] =date created
// parts[2] = number of likes
// parts[3] = source of tweet
// parts[4] = tweet
// parts[5] = sentiment
// parts[6] = modified_timestamp