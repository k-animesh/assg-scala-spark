import org.apache.spark.SparkContext
object topProducts {
def main(args:Array[String]): Unit = {
  val sc = new SparkContext("local[*]","topProducts")
  val data = Seq(
    ("p1", "This product p1 is great. I love p1."),
    ("p2", "p2 is okay, but could be better."),
    ("p1", "Another positive review for p1."),
    ("p3", "p3 didn't meet my expectations."),
    ("p1", "Highly recommend p1 to everyone!"),
    ("p2", "I wouldn't buy p2 again."),
    ("p4", "p4 is the best product ever made!"),
    ("p1", "Yet another review mentioning p1."),
    ("p2", "p2 is not worth the money.")
  )
  val rdd = sc.parallelize(data)
  val rdd1 = rdd.flatMap{case(product_id,review)=>review.toLowerCase().split("\\W+").filter(_.nonEmpty).map(x=>(product_id,1))}
  val rdd2 = rdd1.reduceByKey((x,y)=>(x+y))
  rdd2.collect().foreach(println)





}
}
