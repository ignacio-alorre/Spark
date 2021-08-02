case class User(id: Integer, name: String, score: Array[Map[String, Integer]])

val originalDF = Seq(
new User(1, "Arthur", Array(Map("score" -> 5, "year" -> 2010))),
new User(2, "Trevor", Array(Map("score" -> 30, "year" -> 2014))),
new User(2, "Trevor", Array(Map("score" -> 30, "year" -> 2021))),
new User(4, "Franklin", Array(Map("score" -> 10, "year" -> 2018))),
new User(4, "Franklin", Array(Map("score" -> 12, "year" -> 2015))),
new User(6, "Trevor", Array(Map("score" -> 30, "year" -> 2020)))
).toDF


// Selecting distinct rows based on a single columns
originalDF.dropDuplicates("name").show(false)

+---+--------+--------------------------------+
|id |name    |score                           |
+---+--------+--------------------------------+
|4  |Franklin|[Map(score -> 10, year -> 2018)]|
|2  |Trevor  |[Map(score -> 30, year -> 2014)]|
|1  |Arthur  |[Map(score -> 5, year -> 2010)] |
+---+--------+--------------------------------+



// Select distinct rows based on multiple columns
val unique_cols = List("name", "id")
originalDF.dropDuplicates("name", "id").show(false)  

+---+--------+--------------------------------+
|id |name    |score                           |
+---+--------+--------------------------------+
|2  |Trevor  |[Map(score -> 30, year -> 2014)]|
|6  |Trevor  |[Map(score -> 30, year -> 2020)]|
|4  |Franklin|[Map(score -> 10, year -> 2018)]|
|1  |Arthur  |[Map(score -> 5, year -> 2010)] |
+---+--------+--------------------------------+
 