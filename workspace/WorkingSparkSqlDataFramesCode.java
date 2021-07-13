/*
 * 
 * 
 * 
 * 
$ spark-shell
// sc is the default spark context 
scala> val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
// please note that the data needs to be in hadoop space , specially in this case 
// of spark sql 
scala> val dfs = sqlContext.read.json("/user/cloudera/sparkSqlData.json");
scala> dfs.show;

}*/

// OUTPUT

/*scala> dfs.show;
+---------------+----+----+-------+
|_corrupt_record| age|  id|   name|
+---------------+----+----+-------+
|              {|null|null|   null|
|           null|  25|1201| satish|
|           null|  28|1202|krishna|
|           null|  39|1203|  amith|
|           null|  23|1204|  javed|
|           null|  23|1205| prudvi|
|              }|null|null|   null|
+---------------+----+----+-------+
*/
