package org.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        //var logger  = LoggerFactory.getLogger(App.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromCollection(Collections.singletonList("Hello Word"));
        dataSource.print();
        System.out.println( "Hello World!" );
        //logger.info("hello flink");
    }
}
