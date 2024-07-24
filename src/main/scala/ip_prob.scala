import org.apache.spark.SparkContext
object ip_prob {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "ip")

    val rdd1 = sc.textFile("E:/ip.txt")
    val rdd2 = rdd1.flatMap(x => x.split("\\|"))
    val rdd3 = rdd2.map(x => (x, 1))
    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
    val rdd5 = rdd4.sortBy(x => x._2, false)

    rdd5.take(1).foreach(println)

  }
}
