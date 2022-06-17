// Given a Map[String, String] we want to clean those elements with null values
val mp = Map("k1" -> "111111", "k2" -> "222222", "k3" -> "333333", "k4" -> null)

// Filtering elements with null values 
mp.filter{case(k,v) => v != null}

/*
Output:
Map(k1 -> 111111, k2 -> 222222, k3 -> 333333)
*/

// Converting null values into ""
mp.map{case(k,v) => if(v != null) k -> v else k -> ""}

/*
Output:
Map(k1 -> 111111, k2 -> 222222, k3 -> 333333, k4 -> "")
*/
