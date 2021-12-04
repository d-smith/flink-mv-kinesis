package org.ds.fmvk.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.ds.fmvk.pojos.Quote;

public class QuoteFlatMapper implements FlatMapFunction<String, Quote> {
    @Override
    public void flatMap(String s, Collector<Quote> collector) throws Exception {
        String[] quoteParts = s.split(",");
        if(quoteParts != null && quoteParts.length == 2) {
            collector.collect(new Quote(quoteParts[0], Double.valueOf(quoteParts[1])));
        }
    }
}
