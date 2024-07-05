package org.example;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;

public class write_mood_origineBolt extends BaseRichBolt {
    private FileWriter writerMood_Origine;
    private final List<String> sources = Arrays.asList("Android", "iPhone", "App");

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            writerMood_Origine = new FileWriter("/home/ji/storm/examples/twitter/src/main/resources/mood_origine.txt", true);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String mood = input.getStringByField("mood");
        String source = input.getStringByField("source");
        String[] parts = source.split(" ");

        try {
            if (parts.length >= 3 && sources.contains(parts[2])) {
                writerMood_Origine.write(mood+ " " + parts[2] + "\n");
                writerMood_Origine.flush();
            } else {
                System.out.println("Source does not have enough parts and source are not recognized: " + source);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (writerMood_Origine != null) writerMood_Origine.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields declared
    }
}
