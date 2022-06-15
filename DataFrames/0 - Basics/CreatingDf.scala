// Dataframe with one complex column Array(Map[String, Int])

// OPTION A: Just giving the schema names

val originalDF = Seq(
     (1, "Arthur", Array(Map("score" -> 5, "year" -> 2010))),
     (2, "Trevor", Array(Map("score" -> 30, "year" -> 2014))),
     (3, "Franklin", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))
   ).toDF("id", "user", "score")

originalDF.show(false)

/*
+---+--------+----------------------------------------------------------------+
|id |user    |score                                                           |
+---+--------+----------------------------------------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)]                                 |
|2  |Trevor  |[Map(score -> 30, year -> 2014)]                                |
|3  |Franklin|[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|
+---+--------+----------------------------------------------------------------+
*/


originalDF.printSchema()

/*
root
 |-- id: integer (nullable = false)
 |-- user: string (nullable = true)
 |-- score: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: integer (valueContainsNull = false)
*/



originalDF.schema

/*
org.apache.spark.sql.types.StructType = 
	StructType(
		StructField(id,IntegerType,false), 
		StructField(user,StringType,true), 
		StructField(score,ArrayType(MapType(StringType,IntegerType,false),true),true)
	)
*/


// OPTION B: Defining the schema with a case class

case class User(id: Integer, name: String, score: Array[Map[String, Integer]])

val user1 = User(1, "Arthur", Array(Map("score" -> 5, "year" -> 2010)))
val user2 = User(2, "Trevor", Array(Map("score" -> 30, "year" -> 2014)))
val user3 = User(3, "Franklin", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))

val originalDF = sc.parallelize(Seq(user1, user2, user3)).toDF

+---+--------+----------------------------------------------------------------+
|id |name    |score                                                           |
+---+--------+----------------------------------------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)]                                 |
|2  |Trevor  |[Map(score -> 30, year -> 2014)]                                |
|3  |Franklin|[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|
+---+--------+----------------------------------------------------------------+
