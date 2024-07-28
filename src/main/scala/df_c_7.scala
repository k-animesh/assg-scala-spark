
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_7 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")
    val df1 = scores.withColumn("math_grade",when(col("math_score")>80,"A")
      .when(col("math_score")>=60 && col("math_score")<=80,"B")
      .otherwise("C"))
      .withColumn("english_grade",when(col("english_score")>80,"A")
      .when(col("english_score")>=60 && col("english_score")<=80,"B")
      .otherwise("C"))

    df1.show()




  }

}
