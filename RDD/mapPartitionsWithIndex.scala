val data = sc.parallelize(1 to 50, 4)

// What elements are in each partition
data.mapPartitionsWithIndex{case (i,rows) => Iterator((i+": ("+rows.mkString(",")+")"))}.collect

/*
Array[String] = Array(0: (1,2,3,4,5,6,7,8,9,10,11,12), 1: (13,14,15,16,17,18,19,20,21,22,23,24,25), 2: (26,27,28,29,30,31,32,33,34,35,36,37), 3: (38,39,40,41,42,43,44,45,46,47,48,49,50))
*/


// How many elements are in each partition
data.mapPartitionsWithIndex{case (i,rows) => Iterator((i+": "+rows.size))}.collect

/*
Array[String] = Array(0: 12, 1: 13, 2: 12, 3: 13)
*/


def mySquare(iter: Iterator[Int]) : Iterator[Int] = {
   var res = List[Int]()
   while (iter.hasNext) {
     val next = iter.next
     res = res ::: List(next * next)
   }
   res.iterator
 }


// Squaring each element in the original rdd 
data.mapPartitions(mySquare).collect

/*
Array[Int] = Array(1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361, 400, 441, 484, 529, 576, 625, 676, 729, 784, 841, 900, 961, 1024, 1089, 1156, 1225, 1296, 1369, 1444, 1521, 1600, 1681, 1764, 1849, 1936, 2025, 2116, 2209, 2304, 2401, 2500)
*/




// With map it would be:
data.map(x => x*x).collect

/*
res9: Array[Int] = Array(1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361, 400, 441, 484, 529, 576, 625, 676, 729, 784, 841, 900, 961, 1024, 1089, 1156, 1225, 1296, 1369, 1444, 1521, 1600, 1681, 1764, 1849, 1936, 2025, 2116, 2209, 2304, 2401, 2500)
*/