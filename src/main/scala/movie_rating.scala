import org.apache.spark.SparkContext
object movie_rating {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "product_sales")
    val data = Seq(
      (1, 101, 5.0),
      (1, 102, 4.0),
      (2, 101, 3.0),
      (2, 102, 5.0),
      (3, 101, 4.0),
      (3, 102, 4.5),
      (1, 103, 3.5),
      (2, 103, 4.0),
      (3, 103, 4.0)
    )
    val rdd = sc.parallelize(data)
    val rdd1 = rdd.map{case(_,movie_id,rating)=>(movie_id,(rating,1))}
    val rdd2 = rdd1.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    val rdd3 = rdd2.mapValues{case(total_rating,total_count)=>total_rating/total_count.toDouble}
    val rdd4 = rdd3.sortBy(x=>x._2,ascending = false)
    rdd4.collect().foreach(println)

  }
}