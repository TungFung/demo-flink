package com.example;

import com.example.sink.CustomSink;
import com.example.source.GZIPFileSource;
import com.example.types.TaxiFare;
import com.example.types.TaxiRide;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *  根据 rideId 关联 TaxiRide 和 TaxiFare
 */
public class RidesWithFaresExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 读取 TaxiRide 数据
        KeyedStream<TaxiRide, Long> rides = env.addSource(new GZIPFileSource("data\\taxi\\nycTaxiRides.gz"))
                .map(line -> TaxiRide.fromString(line))
                .filter(ride -> ride.isStart())
                .keyBy(ride -> ride.getRideId());

        // 读取 TaxiFare 数据
        KeyedStream<TaxiFare, Long> fares = env.addSource(new GZIPFileSource("data\\taxi\\nycTaxiFares.gz"))
                .map(line -> TaxiFare.fromString(line))
                .keyBy(fare -> fare.getRideId());

        rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(new CustomSink(1));

        env.execute("RidesWithFares");
    }

    private static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        private ValueState<TaxiRide> rideValueState;// 记住相同的 rideId 对应的 taxi ride 事件
        private ValueState<TaxiFare> fareValueState;// 记住相同的 rideId 对应的 taxi fare 事件

        @Override
        public void open(Configuration parameters) throws Exception {
            rideValueState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("saved ride", TaxiRide.class));
            fareValueState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // 这里是处理相同的 rideId 对应的 Taxi Ride 事件
            // 先要看下 rideId 对应的 Taxi Fare 是否已经存在状态中
            TaxiFare fare = fareValueState.value();
            if (fare != null) { // 说明对应的 rideId 的 taxi fare 事件已经到达
                fareValueState.clear();
                // 输出相同的 rideId 对应的 ride 和 fare 事件
                out.collect(Tuple2.of(ride, fare));
            } else {
                // 先保存 ride 事件
                rideValueState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // 这里是处理相同的 rideId 对应的 Taxi Fare 事件
            // 先要看下 rideId 对应的 Taxi Ride 是否已经存在状态中
            TaxiRide ride = rideValueState.value();
            if (ride != null) {
                rideValueState.clear();
                out.collect(Tuple2.of(ride, fare));
            } else {
                fareValueState.update(fare);
            }
        }
    }
}
