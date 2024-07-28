import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types
object class_prob2 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val ratingsData = Seq(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie", "Rating")
    val df1 = ratingsData.groupBy("Movie").agg(count(col("Rating")),avg(col("Rating")))
    df1.show()





  }
}
