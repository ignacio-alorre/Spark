// Creating a Dataframe

case class User(id: Integer, name: String, last_name : String, score: Array[Map[String, Integer]])

val user1 = User(1, "Arthur", "Red", Array(Map("score" -> 5, "year" -> 2010)))
val user2 = User(null, "Trevor", "Black", Array(Map("score" -> 30, "year" -> 2014)))
val user3 = User(3, null, "Green", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))

val originalDF = sc.parallelize(Seq(user1, user2, user3)).toDF

originalDF.show(false)

/*
+----+------+---------+----------------------------------------------------------------+
|id  |name  |last_name|score                                                           |
+----+------+---------+----------------------------------------------------------------+
|1   |Arthur|Red      |[Map(score -> 5, year -> 2010)]                                 |
|null|Trevor|Black    |[Map(score -> 30, year -> 2014)]                                |
|3   |null  |Green    |[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|
+----+------+---------+----------------------------------------------------------------+
*/

// OPTION A: - We dont need to check for nulls, since those will not be concatenated

val concatenatedDF = originalDF.withColumn("id-name", concat_ws("-", col("id"), col("name"), col("last_name")))

concatenatedDF.show(false)

// Note: Nulls will not be concatenated

/*
+----+------+---------+----------------------------------------------------------------+------------+
|id  |name  |last_name|score                                                           |id-name     |
+----+------+---------+----------------------------------------------------------------+------------+
|1   |Arthur|Red      |[Map(score -> 5, year -> 2010)]                                 |1-Arthur-Red|
|null|Trevor|Black    |[Map(score -> 30, year -> 2014)]                                |Trevor-Black|
|3   |null  |Green    |[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|3-Green     |
+----+------+---------+----------------------------------------------------------------+------------+
*/

// OPTION B: - Keeping null values 

val concatenatedDF = originalDF.withColumn("id-name", concat(when(col("id").isNotNull, 			
				col("id")).otherwise(lit("null")),lit("-"),when(col("name").isNotNull,col("name")).otherwise(lit("null")),lit("-"),
				when(col("last_name").isNotNull, col("last_name")).otherwise(lit("null"))))
				
concatenatedDF.show(false)
	
/*	
+----+------+---------+----------------------------------------------------------------+-----------------+
|id  |name  |last_name|score                                                           |id-name          |
+----+------+---------+----------------------------------------------------------------+-----------------+
|1   |Arthur|Red      |[Map(score -> 5, year -> 2010)]                                 |1-Arthur-Red     |
|null|Trevor|Black    |[Map(score -> 30, year -> 2014)]                                |null-Trevor-Black|
|3   |null  |Green    |[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|3-null-Green     |
+----+------+---------+----------------------------------------------------------------+-----------------+
*/