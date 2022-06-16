// Given a Seq[Map[String,String]] order the Seq based on different logic conditions

val sq_mp = Seq(Map("k" -> "11111", "r" -> "1", "t" -> "5"), Map("k" -> "22222", "r" -> "2", "t" -> "3"), Map("k" -> "33333", "r" -> "1", "t" -> "2"))

// Ordering the Seq[Map] based on one single key 
sq_mp.sortBy(_("r"))

/*
Output:
Seq(Map(k -> 11111, r -> 1, t -> 5), Map(k -> 33333, r -> 1, t -> 2), Map(k -> 22222, r -> 2, t -> 3))
*/

// Ordering the Seq[Map] based on two keys
sq_mp.sortBy(mp => (mp("r"), mp("t")))

/*
Output:
Seq(Map(k -> 33333, r -> 1, t -> 2), Map(k -> 11111, r -> 1, t -> 5), Map(k -> 22222, r -> 2, t -> 3))
*/
 
 
// Ordering the Seq[Map] based on two keys, reverse order 
sq_mp.sortBy(mp => (mp("r"), mp("t")))(Ordering.Tuple2(Ordering.String.reverse, Ordering.String.reverse))

/*
Output:
Seq(Map(k -> 22222, r -> 2, t -> 3), Map(k -> 11111, r -> 1, t -> 5), Map(k -> 33333, r -> 1, t -> 2))
*/