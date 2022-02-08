val colSeq = Seq("id", "name")

val dummyDf = Seq.empty[(String,String)].toDF(colSeq:_*)

dummyDf.show(false)

/*
+---+----+
|id |name|
+---+----+
+---+----+
*/
