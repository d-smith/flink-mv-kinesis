package org.ds.fmvk.application;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.ds.fmvk.functions.QuoteFlatMapper;
import org.ds.fmvk.pojos.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MarketValueCalc {
    private static Logger LOG = LoggerFactory.getLogger(MarketValueCalc.class);
    private static final String region = "us-east-1";
    private static final String inputStreamName = "quotestream";
    private static final String positionsStreamName = "positions";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env, String streamName) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), inputProperties))
                .name(streamName)
                .uid(streamName);
    }

    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> rawQuoteStream = createSourceFromStaticConfig(env, inputStreamName);

        DataStream<Quote> quoteStream = rawQuoteStream
                .flatMap(new QuoteFlatMapper()).name("quote flap mapper").uid("quote flap mapper");
        quoteStream.print();

        DataStream<String> rawPositionStream = createSourceFromStaticConfig(env, positionsStreamName);
        rawPositionStream.print();

        env.execute();
    }
}
