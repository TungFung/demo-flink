package com.example.window;

import com.example.source.CustomSource;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * 按照Event Time执行Window，需要配合WaterMark
 * WaterMark翻译成中文是水印，是一种可以附加给每个Event的隐性属性，其表现形式是时间戳。
 * 通过给每个Event添加水印，就可以通过已进入的数据，反推出未到来的数据的可能性有多大。
 * 通俗点的说就是，更后面的人都到了，前面的人应该都到期了。
 * 实际不知道到齐没有，但是通过后面到来的人反推估计前面的人应该都到了齐，真的还没到也不好意思了，不等了。
 * 所以在给这个时间戳具体赋值时，会设置为当前所有数据中最大的EventTime减一定的间隔，
 * 只有当当前最大EventTime的WaterMark超过窗口结束时间时，这个窗口才能关闭，开始执行计算，不然这个窗口就得一直等。
 * 等后面的Event进来了，把WaterMark的时间往前拉，超过窗口结束时间，然后窗口才关闭，开始执行。
 *
 * 对于错过窗口的Event，默认情况下就不统计了，丢弃了。
 * 当还可以设置allowedLateness允许加一些延迟，再次或多次重新开启窗口计算。
 * 具体为：
 *     第一次当waterMark >= windowEndTime 时就关闭窗口并执行计算
 *     第二次甚至多次，当warterMark < windowEndTime + lateness 时重新打开穿窗口再接收这些数据后，再统计。
 *
 * 还能设置收集那些已经被丢弃的Event, sideOutputLateData。
 *
 * 对于窗口的统计，默认是左闭右开，即EventTime在 [窗口开始时间,窗口结束时间) 内才会统计到，右边是开区间。
 *
 * 对于水印传播到下游有多路聚合的情况时，像多路map然后keyBy聚合在一路的情况，waterMark取多条路中最小的那个。
 */
public class EventTimeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<Tuple3<String, Long, String>> outputTag = new OutputTag<Tuple3<String, Long, String>>("LateDate"){};

        SingleOutputStreamOperator<Tuple2<String, Long>> operator = env.addSource(new CustomSource())
                .map(new MapFunction<String, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> map(String line) throws Exception {
                        String[] strings = line.split(",");
                        return Tuple3.of(strings[0], Long.valueOf(strings[1]), strings[2]);
                    }
                })
                .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks())
                .keyBy(0)
                .timeWindow(Time.seconds(4), Time.seconds(4)) //Time Size, Time slide
                //.allowedLateness(Time.milliseconds(2000)) //允许延迟，错过了第一个窗口后，如果waterMark < windowEndTime + lateness
                .sideOutputLateData(outputTag) //收集迟到被丢弃的数据
                .process(new MyProcessWindowFunction());

        operator.getSideOutput(outputTag).map(new MapFunction<Tuple3<String, Long, String>, String>() {
            @Override
            public String map(Tuple3<String, Long, String> value) throws Exception {
                return "迟到的数据:" + value;
            }
        }).print();

        operator.print();

        env.execute("EventTimeWindowExample");
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
            System.out.println("当前窗口内的元素：" + elements);

            long count = Iterables.size(elements);
            out.collect(Tuple2.of(tuple.getField(0), count));
        }
    }

    private static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, String>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime = 0L;
        private long period = 2000;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - period);
            //return null;
        }

        @Override
        public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
            long currentElementEventTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);
            long currentThreadId = Thread.currentThread().getId();
            System.out.println("当前线程 id : " + currentThreadId
                    + "| event = " + element
                    + "| eventTime:" + dateFormat.format(element.f1) // Event Time
                    + "| currentMaxEventTime:" + dateFormat.format(currentMaxEventTime)  // Max Event Time
                    + "| currentWaterMark:" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermark
            return currentElementEventTime;
        }
    }
}
