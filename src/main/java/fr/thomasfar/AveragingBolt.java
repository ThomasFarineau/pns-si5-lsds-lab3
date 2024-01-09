package fr.thomasfar;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.Map;

public class AveragingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private LinkedList<Integer> recentNumbers;

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.recentNumbers = new LinkedList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        int number = tuple.getIntegerByField("odd-number");
        recentNumbers.add(number);
        if (recentNumbers.size() > 10) {
            recentNumbers.removeFirst();
        }
        if (recentNumbers.size() == 10) {
            double average = recentNumbers.stream().mapToInt(Integer::intValue).average().orElse(0);
            System.out.println("Average of last 10 numbers: " + average);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields for this bolt
    }
}
