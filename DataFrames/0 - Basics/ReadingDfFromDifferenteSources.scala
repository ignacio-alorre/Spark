/*
Three different ways of reading a Dataframe

* Avro file in hdfs
* Parquet file in hdfs 
* Hive table

*/


val sqlContext = spark.sqlContext

val originalDfType = "hive"
			
// Reading Original Dataframe
val originalDf = originalDfType match {
	case "avro" => spark.read.format("com.databricks.spark.avro").load(originalDfLocation)
	case "parquet" => spark.read.parquet(originalDfLocation)
	case "hive" => sqlContext.table(originalDfLocation)
}