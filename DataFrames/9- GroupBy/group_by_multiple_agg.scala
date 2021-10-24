case class Scores(id: Integer, tier: String, score: Integer)

val originalDF = Seq(
new Scores(1, "Green", 25),
new Scores(1, "Green", 87),
new Scores(1, "Orange", 52),
new Scores(1, "Orange", 29),
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
|1  |Orange|29   |
|1  |Red   |16   |
|1  |Pink  |76   |
|1  |null  |93   |
+---+------+-----+
*/

originalDF.groupBy("id", "tier").agg(count("id").as("count_records"), sum("score").as("aggredated_score")).show(false)

/*
+---+------+-------------+----------------+
|id |tier  |count_records|aggredated_score|
+---+------+-------------+----------------+
|1  |null  |1            |93              |
|1  |Green |2            |112             |
|1  |Red   |1            |16              |
|1  |Pink  |1            |76              |
|1  |Orange|2            |81              |
+---+------+-------------+----------------+
*/