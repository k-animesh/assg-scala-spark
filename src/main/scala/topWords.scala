import org.apache.spark.SparkContext
object topWords {
  def main(args:Array[String]): Unit = {

    val sc = new SparkContext("local[*]","topWords")

    // Sample data (replace with your actual RDD creation logic)
    val data = List(
      (1, "Hello world hello"),
      (2, "Hello again world"),
      (3, "Spark is awesome")
    )
    val rdd = sc.parallelize(data)
    val rdd1 = rdd.flatMap{case(id,sentence)=>sentence.toLowerCase().split("\\s+")}.map(word=>(word,1))
    val rdd2 = rdd1.reduceByKey((x,y)=>(x+y))
    val rdd3 = rdd2.sortBy(x=>x._2,ascending = false)
    rdd3.take(1).foreach(println)



  }

}
