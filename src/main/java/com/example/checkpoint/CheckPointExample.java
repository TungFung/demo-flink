package com.example.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 检查点保存策略设置
 * State将在保存点，打个快照保存，具体的保存策略
 */
public class CheckPointExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 设置 checkpoint
        // 开启 checkpoint 功能，checkpoint 的周期是 10 秒
        env.enableCheckpointing(10000);
        // 配置 checkpoint 行为特性
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 设置两个 checkpoint 之间必须间隔一段时间
        // 设置两个 checkpoint 之间最小间隔时间是 30 秒
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        // 设置可以允许多个 checkpoint 一起运行，前提是 checkpoint 不占资源
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        // 可以给 checkpoint 设置超时时间，如果达到了超时时间的话，Flink 会强制丢弃这一次 checkpoint
        // 默认值是 10 分钟
        checkpointConfig.setCheckpointTimeout(30000);
        // 设置即使 checkpoint 出错了，继续让程序正常运行
        // 1.9.0 不建议使用这个参数
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 设置当 flink 程序取消的时候保留 checkpoint 数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置 Flink 程序的自动重启策略
        // 默认：fixed-delay restart strategy，重启的次数是 Integer.MAX_VALUE，重启之间的时间间隔是 10秒
        // 1. fixed-delay restart strategy ：尝试重启多次，每次重启之间有一个时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5, // 重新启动总次数
                Time.seconds(30) // 每次重新启动的时间间隔
        ));
        // 2. failure-rate restart strategy ：
        // 尝试在一个时间段内重启执行次数，每次重启之间也需要一个时间间隔的
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5, // 在指定时段重启的次数
                Time.seconds(30), // 指定的时间段
                Time.seconds(5) // 两次重启之间的时间间隔
        ));
        // 3. no-restart strategy：不进行重启，一旦发生错误立马停止程序
        env.setRestartStrategy(RestartStrategies.noRestart());
    }
}
