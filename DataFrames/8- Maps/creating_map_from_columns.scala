case class Scores(id: Integer, tier: String, score: Integer, reward: Integer)

val originalDF = Seq(
new Scores(1, "Green", 25, 2),
new Scores(1, "Green", 87, 4),
new Scores(1, "Orange", 52, 3),
new Scores(1, "Red", 16, 1),
new Scores(1, "Pink", 76, 3),
new Scores(1, null, 93, 4)
).toDF

originalDF.show(false)

/*
+---+------+-----+------+
|id |tier  |score|reward|
+---+------+-----+------+
|1  |Green |25   |2     |
|1  |Green |87   |4     |
|1  |Orange|52   |3     |
|1  |Red   |16   |1     |
|1  |Pink  |76   |3     |
|1  |null  |93   |4     |
+---+------+-----+------+
*/

// Note map() lowercase to generate the in Spark. But Map() to generate a map in scala
originalDF.withColumn("score_map", map(lit("score"), col("score"), lit("reward"), col("reward"))).show(false)

+---+------+-----+------+-----------------------------+
|id |tier  |score|reward|score_map                    |
+---+------+-----+------+-----------------------------+
|1  |Green |25   |2     |Map(score -> 25, reward -> 2)|
|1  |Green |87   |4     |Map(score -> 87, reward -> 4)|
|1  |Orange|52   |3     |Map(score -> 52, reward -> 3)|
|1  |Red   |16   |1     |Map(score -> 16, reward -> 1)|
|1  |Pink  |76   |3     |Map(score -> 76, reward -> 3)|
|1  |null  |93   |4     |Map(score -> 93, reward -> 4)|
+---+------+-----+------+-----------------------------+

// Handling null values 

val originalDF = Seq(
new Scores(1, "Green", 25, null),
new Scores(1, "Green", 87, 4),
new Scores(1, "Orange", 52, null),
new Scores(1, "Red", 16, 1),
new Scores(1, "Pink", null, 3),
new Scores(1, null, 93, 4)
).toDF

// In case a field is null, we replace the value with 0
originalDF.withColumn("score_map", map(lit("score"), coalesce(col("score"), lit(0)), lit("reward"), coalesce(col("reward"), lit(0)))).show(false)

/*
+---+------+-----+------+-----------------------------+
|id |tier  |score|reward|score_map                    |
+---+------+-----+------+-----------------------------+
|1  |Green |25   |null  |Map(score -> 25, reward -> 0)|
|1  |Green |87   |4     |Map(score -> 87, reward -> 4)|
|1  |Orange|52   |null  |Map(score -> 52, reward -> 0)|
|1  |Red   |16   |1     |Map(score -> 16, reward -> 1)|
|1  |Pink  |null |3     |Map(score -> 0, reward -> 3) |
|1  |null  |93   |4     |Map(score -> 93, reward -> 4)|
+---+------+-----+------+-----------------------------+
*/

