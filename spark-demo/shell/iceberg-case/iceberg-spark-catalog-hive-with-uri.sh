#!/bin/bash
# ice_catalog.db.iceberg_table_2

spark-sql \
--conf spark.sql.catalog.ice_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.ice_catalog.type=hive \
--conf spark.sql.catalog.ice_catalog.uri=thrift://localhost:9083 \
<<EOF

--
--
create table ice_catalog.db.iceberg_table_2(id bigint, data string) using iceberg;
--
--
insert into ice_catalog.db.iceberg_table_2 VALUES(1,'a'), (2,'b'), (3,'c');
--
--
select * from ice_catalog.db.iceberg_table_2;

EOF

echo; echo
hdfs dfs -ls -h -R /root/iceberg_warehouse

echo; echo
mysql -uroot -p123456 <<EOF
select * from hive.DBS;
select * from hive.TBLS;
EOF