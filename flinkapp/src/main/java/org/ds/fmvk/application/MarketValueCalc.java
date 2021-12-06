package org.ds.fmvk.application;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.ds.fmvk.functions.PositionFlatMapper;
import org.ds.fmvk.functions.QuoteEvaluator;
import org.ds.fmvk.functions.QuoteFlatMapper;
import org.ds.fmvk.pojos.MarketValue;
import org.ds.fmvk.pojos.Position;
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
        //quoteStream.print();

        DataStream<String> rawPositionStream = createSourceFromStaticConfig(env, positionsStreamName);
        DataStream<Position> positions = rawPositionStream.flatMap(new PositionFlatMapper());
        positions.print();

        KeyedStream<Position, String> positionsByAccount =
                positions.keyBy((KeySelector<Position,String>) position -> position.owner);

        positionsByAccount.print().uid("positions by account printer").name("positions by account printer");

        MapStateDescriptor<Void,Quote> broadcastDescriptor =
                new MapStateDescriptor<Void, Quote>("quotes", Types.VOID,Types.POJO(Quote.class));

        BroadcastStream<Quote> quotes = quoteStream
                .keyBy(quote -> quote.symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce(new ReduceFunction<Quote>() {
                    @Override
                    public Quote reduce(Quote quote, Quote t1) throws Exception {
                        return t1;
                    }
                }).uid("quote reducer").name("quote reducer")
                .broadcast(broadcastDescriptor);

        //Connect the streams
        DataStream<MarketValue> balanceCalcStream =
                positionsByAccount
                        .connect(quotes)
                        .process(new QuoteEvaluator()).uid("mv calc stream").name("mv calc stream");

        balanceCalcStream.print();

        env.execute();
    }
}
