package org.ds.fmvk.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class QuoteStructureFilter implements FilterFunction<String> {

    @Override
    public boolean filter(String s) throws Exception {
        String[] quoteParts = s.split(",");
        return quoteParts != null && quoteParts.length == 2;
    }
}
