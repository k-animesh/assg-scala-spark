
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_5 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")
    val df1 = orders.withColumn("orderType", when(col("quantity")<10 && col("total_price")<200,"Small&cheap")
      .when(col("quantity")>=10 && col("total_price")<200,"Bulk&Discounted")
    .otherwise("PremiumOrder"))

    df1.show()




  }

}
