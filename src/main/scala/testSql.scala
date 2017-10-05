import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession


object test{

  def main(args: Array[String]): Unit = {
//    val spconf = new SparkConf().setMaster("local").setAppName("test")
//    val sc = new SparkContext(spconf)
//    val sqlContext = new SQLContext(sc)
//    val prop = new java.util.Properties
//    prop.setProperty("user","hieupd")
//    prop.setProperty("password","22X6sL8MM177Z5L")
//    val sql = "jdbc:mysql://192.168.6.7:3306/adn"
//
//    val df = sqlContext.read.jdbc(sql,"adn_banner",prop)
//    df.printSchema()

    val sparkConf = new SparkConf().setAppName("countuser")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.binaryAsString","true")

    //    val spark = SparkSession.builder().master("local").appName("Log Query").enableHiveSupport().getOrCreate()
    val logdata = sqlContext.read.parquet("/home/hadoop/Downloads/parquet_logfile_at_21h_35.snap").repartition(10)
    logdata.registerTempTable("log")
    val sqlClickResult= sqlContext.sql("select guid,domain,path from log ").filter(e => !(e.getString(1)!="kenh14.vn"))
    val a = sqlClickResult.map(a =>  (a.getLong(0), a.getString(1)+""+a.getString(2)))
    val urlkenh14 = sqlContext.read.csv("/home/hadoop/IdeaProjects/kenh14.csv")
    val newNames = Seq("url", "label")
    val dfRenamed = urlkenh14.toDF(newNames: _*)
    val newNames_a = Seq("guid", "url")
    val dfRenamed_a = a.toDF(newNames_a: _*)





    val result = dfRenamed.join(dfRenamed_a).filter(e => e.getInt(2)!=0)
    print("unique visitor 2017_10_02 = " + result.groupBy("guid").count())



  }

}


