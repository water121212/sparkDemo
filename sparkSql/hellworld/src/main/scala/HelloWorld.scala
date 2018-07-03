import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val df = spark.read.json("E:\\ideaWorkSpace\\sparkDemo\\sparkSql\\hellworld\\src\\main\\resources\\person.json")
    df.show()
    df.printSchema()
    //DSL风格查询
    import  spark.implicits._
    df.filter($"age">20).show()
    //sql风格
    df.createOrReplaceTempView("person")
    spark.sql("select *  from person where age > 20").show()
//    spark.sql("select *  from person where age > 21").show()

//    result.write.json("./aa.json")
    spark.stop()
  }

}
