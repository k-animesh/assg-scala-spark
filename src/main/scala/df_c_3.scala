
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_3 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")
    val df1 = documents.withColumn("content_category" ,when(col("content") contains("fox") ,"Animal Related").
      when(col("content") contains("Lorem"), "Placeholder Text")
      .otherwise("Tech Related")
    )

    df1.show()




  }

}
