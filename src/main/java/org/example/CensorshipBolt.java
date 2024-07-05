package org.example;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;

public class CensorshipBolt extends BaseRichBolt {
    private OutputCollector collector;
    private FileWriter writerDateCreated;
    private FileWriter writerContains;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            writerDateCreated = new FileWriter("/home/ji/storm/examples/twitter/src/main/resources/dateCreated.txt", true);
            writerContains = new FileWriter("/home/ji/storm/examples/twitter/src/main/resources/contains.txt", true);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        StringBuilder result = new StringBuilder();
        String text = input.getStringByField("contains");
        String dateCreated = input.getStringByField("dateCreated");
        try {
            synchronized (this) {
                writerDateCreated.write(dateCreated + "\n");
                writerDateCreated.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }

        for (char c : text.toCharArray()) {
            if (c >= 32 && c <= 127) {
                result.append(c);
            } else {
                result.append("*");
            }
        }

        try {
            synchronized (this) {
                writerContains.write(result.toString() + "\n");
                writerContains.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (writerDateCreated != null) writerDateCreated.close();
            if (writerContains != null) writerContains.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }
}


//        declarer.declare(new Fields("dateCreated","source","contains","mood"));

// parts[0] = id
// parts[1] =date created
// parts[2] = number of likes
// parts[3] = source of tweet
// parts[4] = tweet
// parts[5] = sentiment
// parts[6] = modified_timestamp