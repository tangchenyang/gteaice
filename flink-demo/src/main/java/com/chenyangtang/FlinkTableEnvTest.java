package com.chenyangtang;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

public class FlinkTableEnvTest {
    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build()
        );

        CsvTableSource csvTableSource = CsvTableSource.builder()
                .path("file:///Users/twadmin/Downloads/ml-latest-small/student.csv")
                .fieldDelimiter(",")
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("gender", DataTypes.STRING())

                .build();



        tableEnvironment.registerTableSource("sourceTable", csvTableSource);
        Table count_by_gender = tableEnvironment.sqlQuery("select gender, count(1) from  sourceTable group by gender");
        System.out.println(count_by_gender.explain());
        count_by_gender.execute().print();
//        .collect() .forEachRemaining(System.out::println);


        tableEnvironment.execute("");
    }
}
