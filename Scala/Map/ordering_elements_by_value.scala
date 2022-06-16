// Given a Map[String, String] ordering the elements by its value 

import scala.collection.immutable.ListMap

val mp = Map("a" -> "2", "b" -> "5", "c" -> "3", "d" -> "1")

// Ordering the Map by value in ascending order
ListMap(mp.toSeq.sortWith(_._2 < _._2):_*)

/*
Output:
ListMap(d -> 1, a -> 2, c -> 3, b -> 5)
*/

// Ordering the Map by value in descending order
ListMap(mp.toSeq.sortWith(_._2 > _._2):_*)

/*
Output:
ListMap(b -> 5, c -> 3, a -> 2, d -> 1)
*/

// Ordering the Map by key in ascending order 
ListMap(mp.toSeq.sortWith(_._1 < _._1):_*)

/*
Output:
ListMap(a -> 2, b -> 5, c -> 3, d -> 1)
*/
