package com.example.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：累计每个相同key的值
 6> (2,4)
 6> (2,6)
 6> (2,11)
 5> (1,3)
 5> (1,8)
 5> (1,15)
 */
public class ReducingStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            /**
             *  ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
             *      get() 获取状态值
             *      add()  更新状态值，将数据放到状态中
             *      clear() 清除状态
             */
            private ReducingState<Long> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>(
                        "reducingState",
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                        Long.class
                );
                reducingState = super.getRuntimeContext().getReducingState(descriptor);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Long>> out) throws Exception {
                reducingState.add(in.f1);
                out.collect(Tuple2.of(in.f0, reducingState.get()));
            }
        }).print();
        env.execute("ReducingStateExample");
    }



}
