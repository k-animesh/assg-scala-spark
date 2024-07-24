import org.apache.spark.SparkContext
object topStock {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "product_sales")
    val data = Seq(
      ("AAPL", "2023-01-01", 150.0),
      ("AAPL", "2023-01-02", 155.0),
      ("AAPL", "2023-01-03", 160.0),
      ("GOOGL", "2023-01-01", 2500.0),
      ("GOOGL", "2023-01-02", 2550.0),
      ("GOOGL", "2023-01-03", 2600.0),
      ("MSFT", "2023-01-01", 300.0),
      ("MSFT", "2023-01-02", 305.0),
      ("MSFT", "2023-01-03", 310.0)
    )
    val rdd = sc.parallelize(data)
    val rdd1 = rdd.map{case(stock,_,price)=>(stock,(price,1))}
    val rdd2 = rdd1.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    val rdd3 = rdd2.mapValues{case(total_price,total_count)=>total_price/total_count.toDouble}
    val rdd4 = rdd3.sortBy(x=>x._2,ascending = false)
    rdd4.collect().foreach(println)


  }
}
