case class Scores(id: Integer, tier: String, score: Integer)

val originalDF = Seq(
new Scores(1, "Green", 25),
new Scores(1, "Green", 87),
new Scores(1, "Orange", 52),
new Scores(1, "Orange", 29),
new Scores(1, "Red", 16),
new Scores(1, "Pink", 76)
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
+---+------+-----+
*/


// Method 1 of defining a UDF 

val simple_udf = udf((input : String) => {
(input.toLowerCase, input.toUpperCase)
})


originalDF.withColumn("upperLower", simple_udf(col("tier"))).show(false)

/*
+---+------+-----+---------------+
|id |tier  |score|upperLower     |
+---+------+-----+---------------+
|1  |Green |25   |[green,GREEN]  |
|1  |Green |87   |[green,GREEN]  |
|1  |Orange|52   |[orange,ORANGE]|
|1  |Orange|29   |[orange,ORANGE]|
|1  |Red   |16   |[red,RED]      |
|1  |Pink  |76   |[pink,PINK]    |
+---+------+-----+---------------+
*/