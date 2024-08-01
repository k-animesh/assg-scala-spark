import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
object setA_3 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("setA").master("local[*]").getOrCreate()
    import ss.implicits._
    val employees = List(
      (1, "John", 28, 60000),
      (2, "Jane", 32, 75000),
      (3, "Mike", 45, 120000),
      (4, "Alice", 55, 90000),
      (5, "Steve", 62, 110000),
      (6, "Claire", 40, 40000)
    )
    val df = employees.toDF("id","name","age","salary")
    val df1 = df.withColumn("age_group",when(col("age")<30,"Young")
                .when(col("age")>=30 && col("age")<=50,"Mid")
                .otherwise("Senior"))
              .withColumn("salary_range",when(col("salary")>100000,"High")
                .when(col("salary")>=50000 && col("salary")<=100000,"Medium")
                .otherwise("Low"))
    val j_emp = df.filter(col("name").startsWith("J"))
    val e_emp = df.filter(col("name").endsWith("e"))
    val aggDf = df1.groupBy("age_group").agg(
      sum("salary")as("TotalSalary"),
      avg("salary")as("AvgSalary"),
      max("salary")as("MaxSalary"),
      min("salary")as("MinSalary")
    )

    //df1.show()
    //j_emp.show()
    //e_emp.show()
    aggDf.show()
  }
}
