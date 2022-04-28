package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.util.Collector;

// 离线批处理
public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        final String fileName = "F:\\java\\JavaSourceCodeLearning\\Flink\\src\\main\\java\\org\\example\\word-count.txt";
        DataSource<String> dataSource = environment.readTextFile(fileName);
        dataSource
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception{
                        //对读取到的每一行数据按照空格分割
                        String[] split = s.split(" ");
                        //将每个单词放入collector中作为输出,格式类似于{word:1}
                        for (String word : split) {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();
        //environment.execute();
    }
}
