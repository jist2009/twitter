package org.example;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class TrashBolt extends BaseBasicBolt {
    private FileWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            this.writer = new FileWriter("/home/ji/storm/examples/twitter/src/main/resources/Trash.txt", true);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file", e);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String id = input.getStringByField("id");
        try {
            synchronized (this) {
                writer.write(id + "\n");
                writer.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }
}
