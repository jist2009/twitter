package org.example;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

public class TweetSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try{
            FileReader file = new FileReader("/home/ji/storm/data.csv");
            this.reader = new BufferedReader(file);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file"+e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void nextTuple() {
        try{
            String line = reader.readLine();
            if(line != null){
                collector.emit(new Values(line));
                try{
                    Thread.sleep(200);
                } catch( InterruptedException e){
                    throw new RuntimeException("error sleeping",e);
                }
            }
        } catch( IOException e){
            throw new RuntimeException("error reading tuple",e);
        }
    }

    @Override
    public void close(){
        try{
            reader.close();
        } catch(IOException e){
            throw new RuntimeException("error closing file",e);
        }
    }
}

// créer un bolt qui ne garde que les commentaires positifs