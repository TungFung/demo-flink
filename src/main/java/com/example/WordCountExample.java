package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink web ui 默认端口8081
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        //Flink 程序是有一个默认的最大并行度，默认值是 128
//        Integer maxParallelism = parameterTool.getInt("maxParallelism", 6);
//        //设置每个 operator 的并行度（全局范围）
//        Integer parallelism = parameterTool.getInt("parallelism", 2);

        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        env.setMaxParallelism(maxParallelism);
//        env.setParallelism(parallelism);

        /* 输出收到的每一行数据
         * 例如: hello eric ni hao
         */
        DataStreamSource<String> dataStreamSource = env.socketTextStream("master", 9998);
        dataStreamSource.print();

        /* 对每一行数据进行flatMap
         * 1.先按空格提取每一行中的单词
         * 2.对每一个单词，设置1次出现次数
         * 例如：
         * 2> (hello,1)
         * 2> (eric,1)
         * 2> (ni,1)
         * 2> (hao,1)
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOnes = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.toLowerCase().split(" ");
                for(String word: words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //wordOnes.print();


        /* KeyBy相当于group by, 对相同的key进行分组, 分组之后才能进行sum操作

         */
        KeyedStream<Tuple2<String, Integer>, String> wordGroup = wordOnes.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0;
            }
        });
        wordGroup.print();

        //这里的positionToSum是指需要聚合的指标字段的下标,0是指Tuple中的第1个字段String,1指第二个字段Integer
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = wordGroup.sum(1);
        sumResult.print().setParallelism(1);//独立设置这个operator的并行度，不设置就会采用全局范围内的并行度

        env.execute("Streaming Word Count");
    }
}
