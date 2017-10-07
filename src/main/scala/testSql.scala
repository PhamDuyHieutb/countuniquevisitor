import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.countDistinct


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
    val logdata = sqlContext.read.parquet("/data/Parquet/AdnLog/2017_10_02/*").repartition(10)
    logdata.registerTempTable("log")
//    val sqlClickResult= sqlContext.sql("select guid,domain,path,click_or_view from log ").filter(e => !(e.getString(1)!="kenh14.vn"))
//    val a = sqlClickResult.map(a =>  (a.getLong(0), a.getString(1)+""+a.getString(2),a.getBoolean(3)))
//    val urlkenh14 = sqlContext.read.csv("kenh14.csv")
//    val newNames = Seq("url", "label")
//    val dfRenamed = urlkenh14.toDF(newNames: _*)
//    val newNames_a = Seq("guid", "url","click_or_view")
//    val dfRenamed_a = a.toDF(newNames_a: _*)
//
//
//
//
//    val result = dfRenamed_a.join(dfRenamed,Seq("url"))
//    val view = result.count()
//    val filterresult =   result.filter(e => e.getString(2)!="0")
//    val viewlabel_1 = filterresult.count()
//
//    print("unique visitor 2017_10_02 =================== " + filterresult.select("guid").distinct().count()+ "   co "+ viewlabel_1 +" view nhan 1 tren tong "+ view +" view")




    val sqlClickResult = sqlContext.sql("select guid,domain,path,click_or_view from log ")
    val sqlfilter = sqlClickResult.toDF().filter(sqlClickResult("domain").startsWith("kenh14.vn"))
    val a = sqlClickResult.map(a =>  (a.getLong(0), a.getString(1)+""+a.getString(2),a.getBoolean(3)))
    val urlkenh14 = sqlContext.read.load("/home/hieupd/countuser/countuniquevisitor/kenh14.csv")
    val newNames = Seq("url", "label")
    val dfRenamed = urlkenh14.toDF(newNames: _*)
    val newNames_a = Seq("guid", "url","click_or_view")
    val dfRenamed_a = a.toDF(newNames_a: _*)


    val result = dfRenamed_a.join(dfRenamed,Seq("url"))
        val view = result.count()
        val filterresult =   result.filter(result("label").contains("0") )
        val viewlabel_1 = filterresult.count()

        print("unique visitor 2017_10_02 =================== " + filterresult.select("guid").distinct().count()+ "   co "+ viewlabel_1 +" view nhan 1 tren tong "+ view +" view")
  }

}


