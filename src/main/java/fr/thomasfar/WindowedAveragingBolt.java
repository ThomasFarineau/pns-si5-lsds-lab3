package fr.thomasfar;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.stream.Collectors;

public class WindowedAveragingBolt extends BaseWindowedBolt {
    @Override
    public void execute(TupleWindow window) {
        List<Integer> numbers = window.get().stream().map(tuple -> tuple.getIntegerByField("odd-number")).toList();
        double average = numbers.stream().mapToInt(Integer::intValue).average().orElse(0);
        System.out.println("Windowed Average: " + average);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields for this bolt
    }
}
