package com.example.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    val maxParallelism = parameterTool.getInt("maxParallelism", 6)
    val parallelism = parameterTool.getInt("parallelism", 2)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(maxParallelism)
    env.setParallelism(parallelism)

    val dataStream: DataStream[String] = env.socketTextStream("master", 9998)

    val wordCount = dataStream.flatMap(line => line.split(" ")).map(word => (word, 1)).keyBy(0).sum(1)

    wordCount.print()

    env.execute("Streaming Word Count Scala")
  }
}
