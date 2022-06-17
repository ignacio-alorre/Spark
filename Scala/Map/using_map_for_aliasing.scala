// Given a Seq[Map[String, String]] we want alias some of the values based on a master Map[String, String]
val tier_rank_map = Map("GOLD" -> "1", "SILVER" -> "2", "BRONZE" -> "3")

val sq_mp = Seq(Map("k" -> "k1", "tier" -> "GOLD"), Map("k" -> "k2", "tier" -> "SILVER"), Map("k" -> "k3", "tier" -> null), Map("k" -> "k4", "tier" -> "UNKNOWN"))

// In case "tier" is null, I make it ""
val sq_mp_nonulls = sq_mp.map( mp => mp.map{case (k,v) => if(v == null) k -> "" else k -> v})

// In case "tier" is not present in "tier_rank_map" it will return value "4"
sq_mp_nonulls.map(sk => sk.updated("tier", tier_rank_map.getOrElse(sk.getOrElse("tier", "").toUpperCase(), "4")))

/*
Output:
Seq(Map(k -> k1, tier -> 1), Map(k -> k2, tier -> 2), Map(k -> k3, tier -> 4), Map(k -> k4, tier -> 4))
*/