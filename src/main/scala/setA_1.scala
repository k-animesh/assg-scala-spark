
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
object setA_1 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("setA").master("local[*]").getOrCreate()
    import ss.implicits._
    val students =  List(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
    )
    val df = students.toDF("id","name","marks","subject")
    val gradedf = df.withColumn("grade",(when(col("marks") >= 90, "A").when(col("marks") >= 80 && col("marks") < 90, "B")
      .when(col("marks") >= 70 && col("marks") < 80, "C").when(col("marks") >= 60 && col("marks") < 70, "D")
      .otherwise("E")))
    //val window = Window.partitionBy("subject")
    //val avgMarks1 = df.withColumn("AvgMarks",avg("marks").over(window))
    val avgMarks = df.groupBy("subject").avg("marks").as("avg")
    val max = df.groupBy("subject").max("marks")
    val min = df.groupBy("subject").min("marks")
    val countGrade = gradedf.groupBy("subject","grade").count()
    countGrade.show()
    //max.show()
    //min.show()
    //gradedf.show()
    avgMarks.show()
    //avgMarks1.show()




  }

}
