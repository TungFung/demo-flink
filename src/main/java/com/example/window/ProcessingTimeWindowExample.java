package com.example.window;

import com.example.source.CustomSource;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 按照Processing Time执行
 * 即在每个窗口的结束时点，仅对在这个窗口时间范围内接收到的元素做处理。
 */
public class ProcessingTimeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.addSource(new CustomSource())
                .map(new MapFunction<String, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> map(String line) throws Exception {
                        String[] strings = line.split(",");
                        return Tuple3.of(strings[0], Long.valueOf(strings[1]), strings[2]);
                    }
                })
                // 设置获取 Event Time 的逻辑
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5)) //Time Size, Time slide
                .process(new MyProcessWindowFunction())
                .print();

        env.execute("ProcessingTimeWindowExample");
    }

    /**
     * <IN> The type of the input value.
     * <OUT> The type of the output value.
     * <KEY> The type of the key.
     * <W> The type of {@code Window} that this window function can be applied on.
     */
    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple3<String, Long, String>, Tuple2<String, Long>, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, //只有输入元组的第一个元素的值，后面的都没有
                            Context context,
                            Iterable<Tuple3<String, Long, String>> elements,
                            Collector<Tuple2<String, Long>> out) throws Exception {
            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
            System.out.println("当前时间：" + dateFormat.format(System.currentTimeMillis()));
            System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间: " + dateFormat.format(context.window().getStart()) + "窗口结束时间:" + dateFormat.format(context.window().getEnd()));
            System.out.println("当前处理的元素:" + tuple);
            System.out.println("当前窗口内的元素：" + elements);

            long count = Iterables.size(elements);
            out.collect(Tuple2.of(tuple.getField(0), count));
        }
    }
}
