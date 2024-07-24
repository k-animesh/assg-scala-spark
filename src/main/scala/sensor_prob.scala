import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import java.util.Calendar

import java.sql.Timestamp
object sensor_prob {
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext("local[*]","sensor")
    // Function to generate a random timestamp
    def randomTimestamp(): Timestamp = {
      val calendar = Calendar.getInstance()
      calendar.set(Calendar.YEAR, 2024)
      calendar.set(Calendar.MONTH, (Math.random() * 12).toInt)
      calendar.set(Calendar.DAY_OF_MONTH, (Math.random() * 28).toInt + 1)
      calendar.set(Calendar.HOUR_OF_DAY, (Math.random() * 24).toInt)
      calendar.set(Calendar.MINUTE, (Math.random() * 60).toInt)
      calendar.set(Calendar.SECOND, (Math.random() * 60).toInt)
      new Timestamp(calendar.getTimeInMillis)
    }
    // Create some random sensor data
    val data = Seq(
      ("sensor1", randomTimestamp(), Math.random() * 100),
      ("sensor2", randomTimestamp(), Math.random() * 100),
      ("sensor1", randomTimestamp(), Math.random() * 100),
      ("sensor3", randomTimestamp(), Math.random() * 100),
      ("sensor2", randomTimestamp(), Math.random() * 100),
      ("sensor3", randomTimestamp(), Math.random() * 100),
      ("sensor1", randomTimestamp(), Math.random() * 100)
    )
    val sensorRDD:RDD[(String, Timestamp, Double)] = sc.parallelize(data)
    sensorRDD.collect().foreach(println)
    val rdd1 = sensorRDD.map(x=>(x._1,x._3))
    val rdd2 = rdd1.reduceByKey((x,y) => (x+y)/2)
    val rdd3 = rdd2.sortBy(x=>x._2,false)
    rdd3.collect().foreach(println)



  }

}
