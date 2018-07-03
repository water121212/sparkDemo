import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//BYSL00000893,ZHAO,2007-8-23
case class tbStock(ordernumber:String,locationid:String,dateid:String) extends Serializable
case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double) extends Serializable
//2003-1-1,200301,2003,1,1,3,1,1,1,1
case class tbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int) extends Serializable
object practice {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val tbStock_rdd = sc.textFile("E:\\ideaWorkSpace\\sparkDemo\\sparkSql\\doc\\tbStock.txt")
    val tbStockDetail_rdd = sc.textFile("E:\\ideaWorkSpace\\sparkDemo\\sparkSql\\doc\\tbStock.txt")
    val tbDate_rdd = sc.textFile("E:\\ideaWorkSpace\\sparkDemo\\sparkSql\\doc\\tbDate.txt")

    val tbStock_ds = tbStock_rdd.map(_.split(",")).map(x=> tbStock(x(0),x(1),x(2))).toDS()
    val tbStockDetail_ds = tbStockDetail_rdd.map(_.split(",")).map(x=> tbStockDetail(x(0),x(1).toInt,x(2),x(3).trim.toInt,x(4).trim.toDouble,x(5).trim.toDouble)).toDS()
    val tbDate_ds = tbDate_rdd.map(_.split(",")).map(x=> tbDate(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt)).toDS()

    tbStock_ds.createOrReplaceTempView("tbStock")
    tbStockDetail_ds.createOrReplaceTempView("tbStockDetail")
    tbDate_ds.createOrReplaceTempView("tbDate")

    //	计算所有订单中每年的销售单数、销售总额
//    spark.sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) " +
//      "from tbStock a join tbStockDetail b  on a.ordernumber=b.ordernumber join tbDate c on a.dateid = c.dateid group by c.theyear").show()

    spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear").show

    //  计算所有订单每年最大金额订单的销售额

    //  计算所有订单中每年最畅销货品
    spark.stop()
  }
}
