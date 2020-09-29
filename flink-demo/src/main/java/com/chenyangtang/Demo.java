package com.chenyangtang;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.ArrayList;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = environment.fromCollection(new ArrayList<String>() {{
            add("beijing");
            add("hangzhou");
        }});

//        ds.map(new F)
        ds.print();
        environment.execute();
    }
}
