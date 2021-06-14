
case class User(id: Integer, name: String, scores: Map[String, Integer])

val user1 = User(1, "Arthur", Map("rank" -> 5, "year" -> 2010))
val user2 = User(2, "Trevor", Map("rank" -> 30, "year" -> 2014))
val user3 = User(3, "Franklin", Map("rank" -> 12, "year" -> 2015))
val user4 = User(4, "Michael", null)

val originalDF = sc.parallelize(Seq(user1, user2, user3, user4)).toDF

originalDF.show(false)

/*
+---+--------+------------------------------+
|id |name    |scores                        |
+---+--------+------------------------------+
|1  |Arthur  |Map(rank -> 5, year -> 2010)  |
|2  |Trevor  |Map(rank -> 30, year -> 2014) |
|3  |Franklin|Map(rank -> 12, year -> 2015) |
|4  |Michael |null                          |
+---+--------+------------------------------+
*/


val rankDF = originalDF.withColumn("rank", col("scores.rank"))

rankDF.show(false)

/*
+---+--------+-----------------------------+----+
|id |name    |scores                       |rank|
+---+--------+-----------------------------+----+
|1  |Arthur  |Map(rank -> 5, year -> 2010) |5   |
|2  |Trevor  |Map(rank -> 30, year -> 2014)|30  |
|3  |Franklin|Map(rank -> 12, year -> 2015)|12  |
|4  |Michael |null                         |null|
+---+--------+-----------------------------+----+
*/