import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object setA_4 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("setA").master("local[*]").getOrCreate()
    import ss.implicits._
    val movies = List(
      (1, "The Matrix", 9, 136),
      (2, "Inception", 8, 148),
      (3, "The Godfather", 9, 175),
      (4, "Toy Story", 7, 81),
      (5, "The Shawshank Redemption", 10, 142),
      (6, "The Silence of the Lambs", 8, 118)
    )
    val df = movies.toDF("id","name","rating","duration_mins")
    val df1 = df.withColumn("ratingCategory",when(col("rating")>=8,"Excellent")
              .when(col("rating")>=6 && col("rating")<8,"Good")
              .otherwise("Average"))
      .withColumn("durationCategory",when(col("duration_mins")>150,"Long")
              .when(col("duration_mins")>=90 && col("duration_mins")<=150,"Medium")
              .otherwise("Short"))

    val t_mov = df.filter(col("name").startsWith("T"))
    val e_mov = df.filter(col("name").endsWith("e"))
    val window = Window.partitionBy("ratingCategory")
    //val aggDf = df1.groupBy("ratingCategory").agg(
      //sum("duration_mins") as("totalDuration"),
      //avg("duration_mins")as("avgDuration"),
      //max("duration_mins")as("max_duration"),
      //min("duration_mins")as("min_duration") )
    val agg_w = df1.withColumn("total_duration",sum("duration_mins").over(window))
                .withColumn("avgDuration",avg("duration_mins").over(window))
                .withColumn("maxDuration",max("duration_mins").over(window))
                .withColumn("minDuration",min("duration_mins").over(window))
    agg_w.show(false)



      //aggDf.show()
    //df1.show()


  }
}
