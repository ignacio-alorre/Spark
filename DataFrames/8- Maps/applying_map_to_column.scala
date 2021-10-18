case class Scores(id: Integer, tier: String, score: Integer)

val originalDF = Seq(
new Scores(1, "Green", 25),
new Scores(1, "Green", 87),
new Scores(1, "Orange", 52),
new Scores(1, "Red", 16),
new Scores(1, "Pink", 76),
new Scores(1, null, 93)
).toDF

originalDF.show(false)

/*
+---+------+-----+
|id |tier  |score|
+---+------+-----+
|1  |Green |25   |
|1  |Green |87   |
|1  |Orange|52   |
|1  |Red   |16   |
|1  |Pink  |76   |
|1  |null  |93   |
+---+------+-----+
*/

val tierMap = typedLit(Map("Black" -> 1, "Red" -> 2, "Orange" -> 3, "Green" -> 4))

originalDF.withColumn("tier_code", coalesce(tierMap(col("tier")),lit(5))).show(false)

/*
Note:
* If tier is null, then 5 (default value) is generated
* If tier is not among the keys, then 5 (default value) is generated

+---+------+-----+---------+
|id |tier  |score|tier_code|
+---+------+-----+---------+
|1  |Green |25   |4        |
|1  |Green |87   |4        |
|1  |Orange|52   |3        |
|1  |Red   |16   |2        |
|1  |Pink  |76   |5        |
|1  |null  |93   |5        |
+---+------+-----+---------+

*/