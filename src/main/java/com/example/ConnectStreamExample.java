package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 双流连接计算
 * 两边的流都在不断，顺序不固定的输入。一遍的输入如果需要等待匹配另一边的值时，需要通过KeyState来标识，缓存
 * 1.两边的数据流都流完了才会停止，一边流完了，另一边没流完也不会停止
 * 2.一定要keyBy操作，因为这样后面才能使用keyState
 * 3.对于keyState即使物理上是相同的内存对象时，对其添加更新操作不一定是同一个变量。
 * 因为底层采取keyGroup分组存储对应的StateMap，然后再根据StateMap获取对应的状态值。
 * 所以只有当两个key相同时State存储值才是同一个对象，update与clear才操作相同的变量。
 * 本案例中特意添加两个 Tuple2.of("C001","Cat"),Tuple2.of("C001","Cat2") 在最前面，
 * 而另一条流中 C001 比较后才输入进来，此时就能发现keyState中的值出现被覆盖的现象。
 * 但不同key的keyState只出现一次所以怎样都不会覆盖。
 */
public class ConnectStreamExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        System.out.println("MaxParallelism:" + env.getMaxParallelism());
        System.out.println("Parallelism:" + env.getParallelism());

        DataStream<String> aStream = env.fromElements("K001","A001", "B001", "C001", "D001", "A001", "F001", "G001").keyBy(x -> x);
        //DataStream<String> aStream = env.fromElements("F001").keyBy(x -> x); //如果数据没来，或者等到的不是匹配的

        DataStream<Tuple2<String, String>> bStream = env.fromElements(
                Tuple2.of("C001","Cat"),Tuple2.of("C001","Cat2"), Tuple2.of("D001","Dog"),
                Tuple2.of("B001","Boy"), Tuple2.of("A001","Atom"),Tuple2.of("G001","Good")
        ).keyBy(x -> x.f0);
        System.out.println("aStream.getParallelism:" + aStream.getParallelism());
        System.out.println("bStream.getParallelism:" + bStream.getParallelism());

        ConnectedStreams<String, Tuple2<String, String>> connectedStream = aStream.connect(bStream);

        SingleOutputStreamOperator<String> flatMapOperator = connectedStream.flatMap(
                new RichCoFlatMapFunction<String, Tuple2<String, String>, String>() {
            private ValueState<String> aState;
            private ValueState<Tuple2<String, String>> bState;

            @Override
            public void open(Configuration parameters) throws Exception {
                TypeHint<Tuple2<String, String>> typeHint = new TypeHint<Tuple2<String, String>>() {
                };
                TypeInformation<Tuple2<String, String>> tuple2TypeInformation = TypeInformation.of(typeHint);

                aState = getRuntimeContext().getState(new ValueStateDescriptor<>("aState", String.class));
                bState = getRuntimeContext().getState(new ValueStateDescriptor<>("bState", tuple2TypeInformation));
            }

            @Override
            public void flatMap1(String in, Collector<String> out) throws Exception {
                //如果B还没来，先把自己保存先
                if (bState.value() == null) {
                    System.out.println("B还没来，先把自己保存先:" + in + ",state:" + aState + ",stateValue:" + aState.value() + ",threadName:" + Thread.currentThread().getName());
                    aState.update(in);
                }
                //如果B已经来了，那么合并两一起输出
                else {
                    out.collect("输出aStream：" + in + "合并bSteam:" + bState.value().f1);
                    aState.clear();//清空自己之前保存的状态
                }
            }

            @Override
            public void flatMap2(Tuple2<String, String> in, Collector<String> out) throws Exception {
                //如果A还没来，先把自己保存
                if (aState.value() == null) {
                    System.out.println("A还没来，先把自己保存:" + in + ",state:" + bState + ",stateValue:" + bState.value() + ",threadName:" + Thread.currentThread().getName());
                    bState.update(in);
                }
                //如果B已经来了，那么合并两一起输出
                else {
                    out.collect("输出aStream:" + aState.value() + "合并bSteam:" + in.f1);
                    bState.clear();//清空自己之前保存的状态
                }
            }
        });
        System.out.println("flatMapOperator.getParallelism:" + flatMapOperator.getParallelism());

        flatMapOperator.print();

        env.execute("Connect Stream Example");
    }

}
