#!/bin/bash
# ice_catalog.db.iceberg_table_4

spark-sql \
--conf spark.sql.catalog.ice_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.ice_catalog.type=hive \
<<EOF

--
--
create table ice_catalog.db.iceberg_table_4(id bigint, data string) using iceberg;
--
--
insert into ice_catalog.db.iceberg_table_4 VALUES(1,'a'), (2,'b'), (3,'c');
--
--
select * from ice_catalog.db.iceberg_table_4;

EOF

echo; echo
hdfs dfs -ls -h -R /root/iceberg_warehouse

echo; echo
mysql -uroot -p123456 <<EOF
select * from hive.DBS;
select * from hive.TBLS;
EOF