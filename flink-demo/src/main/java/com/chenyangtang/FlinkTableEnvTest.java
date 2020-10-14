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
    static TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inBatchMode()
            .build()
    );
    public static void main(String[] args) throws Exception {

        CsvTableSource csvTableSource = CsvTableSource.builder()
                .path("/Users/twadmin/IdeaProjects/gteaice/flink-demo/src/main/resources/student.csv")
                .fieldDelimiter(",")
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("gender", DataTypes.STRING())
                .build();
        tableEnvironment.registerTableSource("sourceTable", csvTableSource);

        printExplain("select gender, count(1) ct from sourceTable group by gender");
        printExplain("select gender, count(1) ct from sourceTable group by gender having count(1) > 1");

        printExplain("select gender from sourceTable group by gender");
        printExplain("select distinct gender from sourceTable");
        printExplain("select l.* from sourceTable l join sourceTable r on l.id = r.id ");
        printExplain("select l.* from sourceTable l left join sourceTable r on l.id = r.id ");
        printExplain("select l.* from sourceTable l left semi join sourceTable r on l.id = r.id ");

        printExplain("select * from sourceTable order by id desc ");


    }

    static void print(String msg){
        System.out.println(msg);    }

    static void printExplain(String sql){
        print(String.format("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< %s >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", sql));
        print(tableEnvironment.sqlQuery(sql).explain());
    }
}
