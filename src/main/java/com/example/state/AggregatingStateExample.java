package com.example.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：要求输出下面这样的结果
 * (2,Contains：4)
 * (2,Contains：4 and 2)
 * (2,Contains：4 and 2 and 5)
 * (1,Contains：3)
 * (1,Contains：3 and 5)
 * (1,Contains：3 and 5 and 7)
 */
public class AggregatingStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>>() {
            /**
             * 合计状态
             * AggregatingState跟Value、List、Map State很不一样，是通过AggregateFunction来对状态值进行聚合，保留聚合后的值
             */
            private AggregatingState<Long, String> totalStr;//我们这里聚合后的值是字符串

            @Override
            public void open(Configuration parameters) throws Exception {
                // 注册状态
                AggregatingStateDescriptor<Long, String, String> descriptor =
                        new AggregatingStateDescriptor<Long, String, String>(
                                "aggregatingState",  // 状态的名字
                                new AggregateFunction<Long, String, String>() { //聚合函数<IN,ACC,OUT>
                                    @Override
                                    public String createAccumulator() {
                                        return "Contains：";
                                    }

                                    @Override
                                    public String add(Long value, String accumulator) {
                                        if ("Contains：".equals(accumulator)) {
                                            return accumulator + value;
                                        }
                                        return accumulator + " and " + value;
                                    }

                                    @Override
                                    public String getResult(String accumulator) {
                                        return accumulator;
                                    }

                                    @Override
                                    public String merge(String a, String b) {
                                        return a + " and " + b;
                                    }
                                },
                                String.class //状态存储的数据类型
                        );
                totalStr = getRuntimeContext().getAggregatingState(descriptor);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, String>> out) throws Exception {
                totalStr.add(in.f1);
                out.collect(Tuple2.of(in.f0, totalStr.get()));
            }
        }).print();
        env.execute("AggregatingStateExample");
    }



}
