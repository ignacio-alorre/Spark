val originalDF = Seq(
(1, "Arthur", Array(Map("score" -> 5, "year" -> 2010))),
(2, "Trevor", Array(Map("score" -> 30, "year" -> 2014))),
(3, "Franklin", Array(Map("score" -> 12, "year" -> 2015), Map("score" -> 25, "year" -> 2016)))
).toDF("id", "name", "score")

val newGuysDF = Seq(
(4, "arthur  ", 20),
(6, "trevor   ", 15),
(9, "   franklin", 8)
).toDF("id", "name", "max_score")


// The columns involved in the join condition
originalDF.alias("o").join(newGuysDF.alias("n"), trim(upper(col("o.name"))) === trim(upper(col("n.name")))).show(false)