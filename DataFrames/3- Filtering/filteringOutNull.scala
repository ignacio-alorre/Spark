case class User(id: Integer, name: String, score: Array[Map[String, Integer]])

val originalDF = Seq(
new User(1, "Arthur", Array(Map("score" -> 5, "year" -> 2010))),
new User(null, "Trevor", Array(Map("score" -> 30, "year" -> 2014))),
new User(2, "Trevor", Array(Map("score" -> 30, "year" -> 2021))),
new User(4, "Franklin", Array(Map("score" -> 10, "year" -> 2018))),
new User(null, "Franklin", Array(Map("score" -> 12, "year" -> 2015))),
new User(null, "Trevor", Array(Map("score" -> 30, "year" -> 2020)))
).toDF


originalDF.where(!col("id").isNull).show(false)

+---+--------+--------------------------------+
|id |name    |score                           |
+---+--------+--------------------------------+
|1  |Arthur  |[Map(score -> 5, year -> 2010)] |
|2  |Trevor  |[Map(score -> 30, year -> 2021)]|
|4  |Franklin|[Map(score -> 10, year -> 2018)]|
+---+--------+--------------------------------+
