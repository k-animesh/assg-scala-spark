
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object setA_2 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    val products = List(
      (1, "Smartphone", 700, "Electronics"),
      (2, "TV", 1200, "Electronics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    )
    import ss.implicits._
    val df = products.toDF("id","name", "price", "category")
    val price_category = df.withColumn("price_category",when(col("price")>500,"Expensive")
                          .when(col("price")>=200 && col("price")<=500,"Moderate")
                          .when(col("price")<200,"Cheap"))
    val s_product = df.filter(col("name").startsWith("S"))
    val product_end_s = df.filter(col("name").endsWith("s"))
    val productsAgg = df.groupBy("category").agg(
      sum("price") as("Total Price"),
      avg("price") as("Average"),
      max("price") as("Max"),
      min("price") as("Min")
    )
    productsAgg.show()
    //product_end_s.show()
    //s_product.show()
    //price_category.show()




  }

}
