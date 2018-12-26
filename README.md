# hbase-data-insert
multi style insert type

## Userage
package project
```
gradle clean shadowJar -xtest
```
run Spark on Yarn
```
spark-submit --master yarn --conf spark.yarn.tokens.hbase.enabled=true --class com.dounine.hbase.BulkLoad --executor-memory 2G --num-executors 2G --driver-memory 2G    --executor-cores 2 build/libs/hbase-data-insert-1.0.0-SNAPSHOT-all.jar
```
