// Given a Seq[String], generating a Map[String,String] with the number of occurrence of each element in the input Seq[String]

val sq_str = Seq("b","a","a","b","c","d", "a")

sq_str.groupBy(identity).mapValues(_.size)

/*
Output:
Map(b -> 2, a -> 3, c -> 1, d -> 1)
*/