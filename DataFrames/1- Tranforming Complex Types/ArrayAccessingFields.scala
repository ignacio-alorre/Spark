case class User(id: Integer, name: String, scores: Array[Map[String, Integer]])

val user1 = User(1, "Arthur", Array(Map("score" -> 5, "year" -> 2010)))
val user2 = User(2, "Trevor", Array(Map("score" -> 30, "year" -> 2014)))
val user3 = User(3, "Franklin", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))
val user4 = User(4, "Michael", null)

val originalDF = sc.parallelize(Seq(user1, user2, user3, user4)).toDF

originalDF.show(false)

/*
+---+--------+----------------------------------------------------------------+
|id |name    |scores                                                           |
+---+--------+----------------------------------------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)]                                 |
|2  |Trevor  |[Map(score -> 30, year -> 2014)]                                |
|3  |Franklin|[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|
|4  |Michael |null                                                            |
+---+--------+----------------------------------------------------------------+
*/



val firstElementDF = originalDF.withColumn("first_score", when(col("scores").isNotNull,col("scores").getItem(0)).otherwise(null))  

firstElementDF.show(false)

/*
+---+--------+----------------------------------------------------------------+------------------------------+
|id |name    |scores                                                          |first_score                   |
+---+--------+----------------------------------------------------------------+------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)]                                 |Map(score -> 5, year -> 2010) |
|2  |Trevor  |[Map(score -> 30, year -> 2014)]                                |Map(score -> 30, year -> 2014)|
|3  |Franklin|[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|Map(score -> 12, year -> 2015)|
|4  |Michael |null                                                            |null                          |
+---+--------+----------------------------------------------------------------+------------------------------+

*/


val firstElementYearDF = originalDF.withColumn("first_score_year", when(col("scores").isNotNull,col("scores").getItem(0).getItem("rank")).otherwise(null))  

firstElementDF.show(false)

/*
+---+--------+----------------------------------------------------------------+------------------------------+
|id |name    |scores                                                          |first_score                   |
+---+--------+----------------------------------------------------------------+------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)]                                 |Map(score -> 5, year -> 2010) |
|2  |Trevor  |[Map(score -> 30, year -> 2014)]                                |Map(score -> 30, year -> 2014)|
|3  |Franklin|[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|Map(score -> 12, year -> 2015)|
|4  |Michael |null                                                            |null                          |
+---+--------+----------------------------------------------------------------+------------------------------+
*/
