// Generating an RDD

case class user(id: Integer, name: String, score: Integer)

val t1 = user(5,"Trevor",1000)
val t2 = user(15,"Michael",500)
val t3 = user(25,"Franklin",1500)

val users = sc.parallelize(Seq(t1, t2, t3))


// Filtering the RDD
val tops = users.filter(_.score >= 1000)

// Printing out the results
tops.collect()

/*
res2: Array[user] = Array(user(5,Trevor,1000), user(25,Franklin,1500))
*/
