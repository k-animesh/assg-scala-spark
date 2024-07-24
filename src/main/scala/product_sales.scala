import org.apache.spark.SparkContext
object product_sales {
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext("local[*]","product_sales")
    val data = Seq(
      ("p1", "2023-01-01", 100),
      ("p2", "2023-01-01", 150),
      ("p1", "2023-01-02", 75),
      ("p2", "2023-01-02", 120),
      ("p1", "2023-01-03", 200)
    )
    val rdd = sc.parallelize(data)
    val rdd1 = rdd.map(x=>(x._1,x._3))
    val rdd2 = rdd1.reduceByKey((x,y)=>(x+y))
    rdd2.collect().foreach(println)





  }

}
