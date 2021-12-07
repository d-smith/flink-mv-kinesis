package org.ds.fmvk.application;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.ds.fmvk.functions.QuoteFlatMapper;
import org.ds.fmvk.pojos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class QuoteConflator {

    private static final String region = System.getenv("AWS_REGION");
    private static final String inputStreamName = "quotestream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
                .name("fmvk input")
                .uid("fmvk input");
    }


    Logger LOG = LoggerFactory.getLogger(QuoteConflator.class);

    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> input = createSourceFromStaticConfig(env);


        DataStream<Quote> quoteStream = input
                .flatMap(new QuoteFlatMapper()).name("quote flap mapper").uid("quote flap mapper");


        quoteStream.keyBy(quote -> quote.symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Quote>() {
                    @Override
                    public Quote reduce(Quote quote, Quote t1) throws Exception {
                        return t1;
                    }
                }).name("quote reducer").uid("quote reducer")
                .map(new MapFunction<Quote, String>() {
                    @Override
                    public String map(Quote quote) throws Exception {
                        return "windowed -> " + quote.toString();
                    }
                }).name("quote to string mapper").uid("quote to string mapper")
                .print().name("quote printer").uid("quote printer");
        env.execute();
    }
}

