import org.apache.spark.SparkContext
object max_sensor_value {
def main(args:Array[String]): Unit = {
  val sc = new SparkContext("local[*]","maxSensorData")
  val sensorData = List(
    (1, 100L, 20),
    (2, 110L, 18),
    (1, 120L, 22),
    (2, 130L, 19),
    (1, 140L, 23)
  )
  val rdd = sc.parallelize(sensorData)
  val rdd1 = rdd.map{case(sensor_id,_,values)=>(sensor_id,values)}
  val rdd2 = rdd1.reduceByKey(math.max)
  rdd2.collect().foreach(println)








}

}
