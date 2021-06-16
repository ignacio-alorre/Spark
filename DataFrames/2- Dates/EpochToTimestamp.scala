case class Log(log_id: Integer, log_type: String, epoch: String)

val log1 = Log(1, "WARNING", "1487138548")
val log2 = Log(2, "ERROR", "1487167548")
val log3 = Log(3, "WARNING", null)

val originalDF = sc.parallelize(Seq(log1, log2, log3)).toDF

/*
+------+--------+----------+
|log_id|log_type|     epoch|
+------+--------+----------+
|     1| WARNING|1487138548|
|     2|   ERROR|1487167548|
|     3| WARNING|      null|
+------+--------+----------+
*/


// Converting Epoch to TimeStamp

originalDF.withColumn("timestamp", from_unixtime(col("epoch"),"dd-MM-yyyy HH:mm:ss")).show


/*
+------+--------+----------+-------------------+
|log_id|log_type|     epoch|          timestamp|
+------+--------+----------+-------------------+
|     1| WARNING|1487138548|15-02-2017 10:02:28|
|     2|   ERROR|1487167548|15-02-2017 18:05:48|
|     3| WARNING|      null|               null|
+------+--------+----------+-------------------+
*/