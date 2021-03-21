package com.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 利用输入控制流的提示数字，如果超过这个长度的数据流字符串将不被打印输出，以达到实时控制的目的。
 * 打开nc -lk 5001 输入数据流，随便输入一串字符
 * 打开nc -lk 5002 输入控制流，输入 length 10 或者 length 5 这种空格分隔的格式
 * 将控制流转为广播流，数据流连接广播流，双流process
 */
public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 5001);//数据流
        DataStreamSource<String> controlStreamSource = env.socketTextStream("localhost", 5002);//控制流

        // 解析控制流中的数据为二元组
        DataStream<Tuple2<String, Integer>> controlStream = controlStreamSource.map(s -> {
            String[] strings = s.split(" ");
            return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
        });

        //将控制流转为广播流
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>("lengthControl", String.class, Integer.class);
        BroadcastStream<Tuple2<String, Integer>> broadcastStream = controlStream.broadcast(descriptor);

        //数据流连接广播流，双流process
        dataStreamSource.connect(broadcastStream).process(new LineLengthLimitProcessor()).print();
        env.execute("BroadcastStateTest");
    }

    private static class LineLengthLimitProcessor extends BroadcastProcessFunction<String, Tuple2<String, Integer>, String> {
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>("lengthControl", String.class, Integer.class);

        @Override
        public void processBroadcastElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(descriptor).put(value.f0, value.f1);//value是控制流的数据
            System.out.println(Thread.currentThread().getName() + " 接收到控制信息 ： " + value);
        }

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // 从 broadcast state 中拿到控制信息
            Integer length = ctx.getBroadcastState(descriptor).get("length");
            if (length == null || value.length() <= length) {
                out.collect(value);
            }
        }
    }

}
