/*
Three different ways of reading a Dataframe

* Avro file in hdfs
* Parquet file in hdfs 
* Hive table

Note: For the avro type it may be required to add some flags and jars during the execution of the spark shell.
* The jar is required to read avro files
* The conf flag is required when reading avros produced by hive, since they usually come without extension

spark2-shell 
--jars spark-avro_2.11-4.0.0.jar 
--conf spark.hadoop.avro.mapred.ignore.inputs.without.extension=false

*/


val sqlContext = spark.sqlContext

val originalDfType = "hive"
			
// Reading Original Dataframe
val originalDf = originalDfType match {
	case "avro" => spark.read.format("com.databricks.spark.avro").load(originalDfLocation)
	case "parquet" => spark.read.parquet(originalDfLocation)
	case "hive" => sqlContext.table(originalDfLocation)
}
