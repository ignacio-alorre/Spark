case class Scores(id: Integer, cat: String, score: Integer)

val originalDF = Seq(
new Scores(1, "cat1", 25),
new Scores(1, "cat1", 87),
new Scores(1, "cat1", 52),
new Scores(1, "cat2", 16),
new Scores(1, "cat2", 93),
new Scores(2, "cat2", 24),
new Scores(2, "cat2", 56),
new Scores(2, "cat2", 78)
).toDF

originalDF.show(false)

/*
+---+----+-----+
|id |cat |score|
+---+----+-----+
|1  |cat1|25   |
|1  |cat1|87   |
|1  |cat1|52   |
|1  |cat2|16   |
|1  |cat2|93   |
|2  |cat2|24   |
|2  |cat2|56   |
|2  |cat2|78   |
+---+----+-----+
*/


// Counting how many records each id has per category
val pivotDF = originalDF.groupBy("id").pivot("cat").count()
pivotDF.show()

/*
+---+----+----+
| id|cat1|cat2|
+---+----+----+
|  1|   3|   2|
|  2|null|   3|
+---+----+----+

*/


// Summing the scores each id has per category
val pivotDF = originalDF.groupBy("id").pivot("cat").sum("score")
pivotDF.show()

/*
+---+----+----+
| id|cat1|cat2|
+---+----+----+
|  1| 164| 109|
|  2|null| 158|
+---+----+----+
*/
