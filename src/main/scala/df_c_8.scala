
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_8 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_add")
    val df1 = emails.withColumn("email_domain", when(col("email_add") contains("gmail"),"GMAIL")
      .when(col("email_add") contains("yahoo"),"YAHOO")
      .when(col("email_add") contains("hotmail"),"HOTMAIL")
    )
    df1.show()




  }

}
