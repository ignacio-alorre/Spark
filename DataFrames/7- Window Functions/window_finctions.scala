case class Scores(id: Integer, date: String, score: Integer)

val originalDF = Seq(
new Scores(1, "2021-01-05", 25),
new Scores(1, "2021-02-07", 87),
new Scores(1, "2021-03-02", 52),
new Scores(1, "2021-08-22", 16),
new Scores(1, "2021-10-15", 93),
new Scores(2, "2021-01-05", 24),
new Scores(2, "2021-07-06", 56),
new Scores(2, "2021-08-07", 78)
).toDF

+---+----------+-----+
|id |date      |score|
+---+----------+-----+
|1  |2021-01-05|25   |
|1  |2021-02-07|87   |
|1  |2021-03-02|52   |
|1  |2021-08-22|16   |
|1  |2021-10-15|93   |
|2  |2021-01-05|24   |
|2  |2021-07-06|56   |
|2  |2021-08-07|78   |
+---+----------+-----+



// Generating a row number based on date
import org.apache.spark.sql.expressions.Window
val windMemberDate = Window.partitionBy(col("id")).orderBy("date")

originalDF.withColumn("rn", row_number() over windMemberDate).show(false)

/*
+---+----------+-----+---+
|id |date      |score|rn |
+---+----------+-----+---+
|1  |2021-01-05|25   |1  |
|1  |2021-02-07|87   |2  |
|1  |2021-03-02|52   |3  |
|1  |2021-08-22|16   |4  |
|1  |2021-10-15|93   |5  |
|2  |2021-01-05|24   |1  |
|2  |2021-07-06|56   |2  |
|2  |2021-08-07|78   |3  |
+---+----------+-----+---+
*/

// Summing total score per id
val windMember = Window.partitionBy(col("id"))

originalDF.withColumn("total_score", sum("score") over windMember).show(false)

/*
+---+----------+-----+-----------+
|id |date      |score|total_score|
+---+----------+-----+-----------+
|1  |2021-01-05|25   |273        |
|1  |2021-02-07|87   |273        |
|1  |2021-03-02|52   |273        |
|1  |2021-08-22|16   |273        |
|1  |2021-10-15|93   |273        |
|2  |2021-01-05|24   |158        |
|2  |2021-07-06|56   |158        |
|2  |2021-08-07|78   |158        |
+---+----------+-----+-----------+
*/

// Calculating Accumulative score based on date 
val windMemberDateAcc = Window.partitionBy(col("id")).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

originalDF.withColumn("acc_score", sum("score") over windMemberDateAcc).show(false)

/*
+---+----------+-----+-----------+
|id |date      |score|	acc_score|
+---+----------+-----+-----------+
|1  |2021-01-05|25   |25         |
|1  |2021-02-07|87   |112        |
|1  |2021-03-02|52   |164        |
|1  |2021-08-22|16   |180        |
|1  |2021-10-15|93   |273        |
|2  |2021-01-05|24   |24         |
|2  |2021-07-06|56   |80         |
|2  |2021-08-07|78   |158        |
+---+----------+-----+-----------+
*/
