

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



val explodedDF = originalDF.withColumn("score", explode(col("scores"))).drop("scores")

explodedDF.show(false)

/*
+---+--------+------------------------------+
|id |name    |score                         |
+---+--------+------------------------------+
|1  |Arthur  |Map(score -> 5, year -> 2010) |
|2  |Trevor  |Map(score -> 30, year -> 2014)|
|3  |Franklin|Map(score -> 12, year -> 2015)|
|3  |Franklin|Map(score -> 25, year -> 2016)|
+---+--------+------------------------------+
*/

