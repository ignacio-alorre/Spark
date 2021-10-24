case class Scores(id: Integer, tier: String, score: Integer, reward: Integer)

// Handling null values 

val originalDF = Seq(
new Scores(1, "Green", 25, null),
new Scores(1, "Green", null, 4),
new Scores(1, "Orange", 52, null),
new Scores(1, "Red", 16, 1),
new Scores(1, "Pink", null, 3),
new Scores(1, null, 93, 4)
).toDF

// In case a field is null, we replace the value with 0
val originalDF_map = originalDF.withColumn("score_map", map(lit("score"), coalesce(col("score"), lit(0)), lit("reward"), coalesce(col("reward"), lit(0))))

originalDF_map.show(false)

/*
+---+------+-----+------+-----------------------------+
|id |tier  |score|reward|score_map                    |
+---+------+-----+------+-----------------------------+
|1  |Green |25   |null  |Map(score -> 25, reward -> 0)|
|1  |Green |null |4     |Map(score -> 0, reward -> 4) |
|1  |Orange|52   |null  |Map(score -> 52, reward -> 0)|
|1  |Red   |16   |1     |Map(score -> 16, reward -> 1)|
|1  |Pink  |null |3     |Map(score -> 0, reward -> 3) |
|1  |null  |93   |4     |Map(score -> 93, reward -> 4)|
+---+------+-----+------+-----------------------------+
*/

import scala.collection.immutable.ListMap

// Sorting the map in ascending order, from first element selecting the value
val highestValueInMap = udf((input: Map[String, Integer]) => {
ListMap(input.toSeq.sortWith(_._2 > _._2):_*).head._2
})

originalDF_map.withColumn("max_score_reward", highestValueInMap(col("score_map"))).show(false)

/*
+---+------+-----+------+-----------------------------+----------------+
|id |tier  |score|reward|score_map                    |max_score_reward|
+---+------+-----+------+-----------------------------+----------------+
|1  |Green |25   |null  |Map(score -> 25, reward -> 0)|25              |
|1  |Green |null |4     |Map(score -> 0, reward -> 4) |4               |
|1  |Orange|52   |null  |Map(score -> 52, reward -> 0)|52              |
|1  |Red   |16   |1     |Map(score -> 16, reward -> 1)|16              |
|1  |Pink  |null |3     |Map(score -> 0, reward -> 3) |3               |
|1  |null  |93   |4     |Map(score -> 93, reward -> 4)|93              |
+---+------+-----+------+-----------------------------+----------------+
*/