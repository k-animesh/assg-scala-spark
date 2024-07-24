import org.apache.spark.SparkContext
object cust_revenue {
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext("local[*]","scala_spark")
    val orders = List(
      (1, 101, "2023-07-01", 50.0),
      (2, 102, "2023-07-02", 30.0),
      (3, 101, "2023-07-03", 20.0),
      (4, 103, "2023-07-04", 70.0),
      (5, 102, "2023-07-05", 60.0)
    )
    val rdd = sc.parallelize(orders)
    val rdd1 = rdd.map(order => (order._2 , order._4))
    val rdd2 = rdd1.reduceByKey((x,y) => (x+y))
    rdd2.collect().foreach(println)



  }

}
