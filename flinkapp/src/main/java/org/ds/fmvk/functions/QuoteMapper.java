package org.ds.fmvk.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.ds.fmvk.pojos.Quote;


public class QuoteMapper implements MapFunction<String, Quote> {

    @Override
    public Quote map(String s) throws Exception {
        String[] quoteParts = s.split(",");
        return new Quote(quoteParts[0], Double.valueOf(quoteParts[1]));
    }
}