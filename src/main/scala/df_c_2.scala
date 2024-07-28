
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_2 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5))
    import ss.implicits._
    val df = reviews.toDF("review_id", "review")
    val df1 = df.withColumn("feedback" ,when(col("review")<3 ,"Bad").
      when(col("review") ===3 || col("review") === 4 , "Good")
      .otherwise("Excellent")
    ).withColumn("is_positive",when(col("review")>=3,"true").otherwise("false"))

    df1.show()




  }

}
