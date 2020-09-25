package com.changwu.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimplePartitioner implements Partitioner{

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // 这里的第二个参数即使发送的Key
        // 我们可以根据这个Key定义负载均衡的粗略
        // 方法返回值为 partition的编号，如：1，表示数据打向 partition = 1的位置
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

    }



}
