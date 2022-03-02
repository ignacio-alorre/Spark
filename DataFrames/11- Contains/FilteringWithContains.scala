// Creating a Dataframe

case class User(id: Integer, name: String, last_name : String, score: Array[Map[String, Integer]])

val user1 = User(1, """Arthur:"Leader"""", "Red", Array(Map("score" -> 5, "year" -> 2010)))
val user2 = User(null, """Trevor:"Follower"""", "Black", Array(Map("score" -> 30, "year" -> 2014)))
val user3 = User(3, null, "Green", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))

val originalDF = sc.parallelize(Seq(user1, user2, user3)).toDF

originalDF.show(false)

/*
+----+-----------------+---------+----------------------------------------------------------------+
|id  |name             |last_name|score                                                           |
+----+-----------------+---------+----------------------------------------------------------------+
|1   |Arthur:"Leader"  |Red      |[Map(score -> 5, year -> 2010)]                                 |
|null|Trevor:"Follower"|Black    |[Map(score -> 30, year -> 2014)]                                |
|3   |null             |Green    |[Map(score -> 12, year -> 2015), Map(score -> 25, year -> 2016)]|
+----+-----------------+---------+----------------------------------------------------------------+
*/

originalDF.where(col("name").contains(""":"Leader"""")).show(false)

/*
+---+---------------+---------+-------------------------------+
|id |name           |last_name|score                          |
+---+---------------+---------+-------------------------------+
|1  |Arthur:"Leader"|Red      |[Map(score -> 5, year -> 2010)]|
+---+---------------+---------+-------------------------------+
*/