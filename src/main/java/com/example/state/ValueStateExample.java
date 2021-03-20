package com.example.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候
 *  就计算这些元素的 value 的平均值。
 *  计算 keyed stream 中每 3 个元素的 value 的平均值
 */
public class ValueStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>() {
            /**
             *  ValueState<T> ：这个状态为每一个 key 保存一个值
             *      value() 获取状态值
             *      update() 更新状态值
             *      clear() 清除状态
             */
            private ValueState<Tuple2<Long, Long>> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                TypeInformation<Tuple2<Long, Long>> typeInformation = Types.TUPLE(Types.LONG, Types.LONG);
                ValueStateDescriptor<Tuple2<Long, Long>> stateDescriptor = new ValueStateDescriptor<>("CountAndSumState", typeInformation);
                state = super.getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Double>> out) throws Exception {
                Tuple2<Long, Long> currentValue = state.value();//获取状态
                if(currentValue == null) {
                    currentValue = Tuple2.of(0L, 0L);//初始化
                }

                //f0是key, 累加key的出现次数，因为之前经过keyBy然后再进来这个flatMap,所以这里的key都是相同的,即相同key出现过的次数
                currentValue.f0 += 1;

                //f1是每个key对应的值，累加这些值
                currentValue.f1 += in.f1;

                state.update(currentValue);//更新状态

                // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
                if(currentValue.f0 >= 3) {
                    double avg = (double) currentValue.f1 / currentValue.f0;
                    out.collect(Tuple2.of(in.f0, avg));
                    state.clear();//清空状态
                }
            }
        }).print();
        env.execute("ValueStateExample");
    }
}
