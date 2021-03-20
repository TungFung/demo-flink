package com.example.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候
 *  就计算这些元素的 value 的平均值。
 *  计算 keyed stream 中每 3 个元素的 value 的平均值
 */
public class ListStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>() {
            /**
             *  ListState<T> ：这个状态为每一个 key 保存集合的值
             *      get() 获取状态值
             *      add() / addAll() 更新状态值，将数据放到状态中
             *      clear() 清除状态
             */
            private ListState<Tuple2<Long, Long>> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                TypeInformation<Tuple2<Long, Long>> typeInformation = Types.TUPLE(Types.LONG, Types.LONG);
                ListStateDescriptor<Tuple2<Long, Long>> stateDescriptor = new ListStateDescriptor<>("CountAndSumState", typeInformation);
                listState = super.getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Double>> out) throws Exception {
                if(listState.get() == null) {
                    listState.addAll(Collections.emptyList());//初始化
                }

                listState.add(in);//更新状态

                List<Tuple2<Long, Long>> allElements = Lists.newArrayList(listState.get());

                // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
                if(allElements.size() >= 3) {
                    int count = 0;
                    int sum = 0;
                    for(Tuple2<Long, Long> elem: allElements) {
                        count ++;
                        sum += elem.f1;
                    }
                    double avg = (double) sum / count;
                    out.collect(Tuple2.of(in.f0, avg));
                    listState.clear();//清空状态
                }
            }
        }).print();
        env.execute("ListStateExample");
    }
}
