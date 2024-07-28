
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_1 {
  def main(args:Array[String]):Unit={
  val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    )
    import ss.implicits._
      val df = employees.toDF("employee_id", "age", "salary")
    val df1 = df.withColumn("category" ,when(col("age")<30 && col("salary")<35000,"Young & lowsalary").
      when(col("age") >= 30 && col("age") <= 40 && col("salary") >= 35000 && col("salary") <= 45000, "Middle Aged & Medium Salary")
      .otherwise("Old & HighSalary"))

      df1.show()




  }

}
