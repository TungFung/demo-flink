package com.example.checkpoint;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyState的存储后端技术实现，设置StateBackend
 * Memory：存储在内存中
 * Fs：存储在分布式文件存储系统中
 * RocksDB：存储在运行时TaskManager所在的RocksDB中，不定期的同步到分布式文件系统中
 */
public class StateBackEndExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(100 * 1024 * 1024);// 默认的话是 5 M
        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:9001/checkpoint-path/");
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://master:9001/checkpoint-path/");
        env.setStateBackend(rocksDBStateBackend);
    }
}
