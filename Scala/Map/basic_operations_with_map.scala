/*
	Different operations with maps in scala
*/

var mp = Map("key1" -> "value1", "key2" -> "value2")

// Accesing key from map
println(mp("key1"))
// value1

// Adding new key to map
mp ++= Map("key3" -> "value3")
println(mp)
// Map(key1 -> value1, key2 -> value2, key3 -> value3)

// Removing key from map
mp -= "key2"
println(mp)
// Map(key1 -> value1, key3 -> value3)

// Replacing an existing key
mp ++= Map("key1" -> "new_value1")
println(mp)
// Map(key1 -> new_value1, key3 -> value3)