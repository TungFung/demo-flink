package com.example.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候
 *  就计算这些元素的 value 的平均值。
 *  计算 keyed stream 中每 3 个元素的 value 的平均值
 */
public class MapStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>() {
            /**
             *  MapState<K, V> ：这个状态为每一个 key 保存一个 Map 集合
             *      put() 将对应的 key 的键值对放到状态中
             *      values() 拿到 MapState 中所有的 value
             *      clear() 清除状态
             */
            private MapState<String, Long> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Long> stateDescriptor = new MapStateDescriptor<>("CountAndSumState", String.class, Long.class);
                mapState = super.getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Double>> out) throws Exception {
                mapState.put(UUID.randomUUID().toString(), in.f1);//key需要是唯一的，所以不能放我们的in.f0,因为不唯一

                List<Long> elems = Lists.newArrayList(mapState.values());

                // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
                if(elems.size() >= 3) {
                    int count = 0;
                    int sum = 0;
                    for(Long elem: elems) {
                        count ++;
                        sum += elem;
                    }
                    double avg = (double) sum / count;
                    out.collect(Tuple2.of(in.f0, avg));
                    mapState.clear();//清空状态
                }
            }
        }).print();
        env.execute("MapStateExample");
    }
}
