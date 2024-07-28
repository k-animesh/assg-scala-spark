
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_4 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

    val df1 = tasks.withColumn("date_diff",when(datediff(col("end_date"),col("start_date")) < 7,"short")
      .when(datediff(col("end_date"),col("start_date")) >= 7 && datediff(col("end_date"),col("start_date"))<=14,"Medium")
      .otherwise("Long")
    )

    df1.show()




  }

}
