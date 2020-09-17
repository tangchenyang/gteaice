#!/bin/bash

base_path=$(cd $(dirname $0) && pwd)
cd $base_path
bash install_java
bash install_hadoop
bash install_mysql
bash install_hive
bash install_scala
bash install_spark
bash install_flink