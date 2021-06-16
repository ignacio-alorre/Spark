// Creating an RDD 

val t1 = Array(5,8,9)
val t2 = Array(15,12,9)
val t3 = Array(25,8,19)

val celsiusRDD = sc.parallelize(Seq(t1, t2, t3))

celsiusRDD.collect().foreach(row => println(row.toList))

/*
List(5, 8, 9)
List(15, 12, 9)
List(25, 8, 19)
*/

val kelvinFactor = 273.15

val kelvinRDD = celsiusRDD.map(row => row.map(_ + kelvinFactor))

kelvinRDD.collect().foreach(row => println(row.toList))

/*
List(278.15, 281.15, 282.15)
List(288.15, 285.15, 282.15)
List(298.15, 281.15, 292.15)
*/

val kelvinFlatRDD = celsiusRDD.flatMap(row => row.map(_ + kelvinFactor))

kelvinFlatRDD.collect().foreach(row => println(row))

/*
278.15
281.15
282.15
288.15
285.15
282.15
298.15
281.15
292.15
*/