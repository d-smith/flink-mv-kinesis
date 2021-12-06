package org.ds.fmvk.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.ds.fmvk.pojos.Position;

public class PositionFlatMapper implements FlatMapFunction<String, Position> {
    @Override
    public void flatMap(String s, Collector<Position> collector) throws Exception {
        try {
            String[] tokens = s.split(",");
            Double.valueOf(tokens[2]);
            if(tokens.length == 3) {
                collector.collect(
                        new Position(tokens[0], tokens[1], Double.valueOf(tokens[2]))
                );
            }
        } catch(Throwable t) {
            return;
        }
    }
}
