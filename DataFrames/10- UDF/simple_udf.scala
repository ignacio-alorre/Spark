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


// Method 1 of defining a UDF 
val myUDF = udf(( input: String) => {
if(input == null)
	null 
else
	input.toLowerCase
})

// Method 2 of defining a function, then converting it into UDF
def myFunc: (String => String) = { s => if(s != null){s.toLowerCase}else{s}}

import org.apache.spark.sql.functions.udf
val myUDF2 = udf(myFunc)


originalDF.withColumn("tier_lower", myUDF(col("tier"))).show(false)

/*
+---+------+-----+----------+
|id |tier  |score|tier_lower|
+---+------+-----+----------+
|1  |Green |25   |green     |
|1  |Green |87   |green     |
|1  |Orange|52   |orange    |
|1  |Orange|29   |orange    |
|1  |Red   |16   |red       |
|1  |Pink  |76   |pink      |
|1  |null  |93   |null      |
+---+------+-----+----------+
*/

