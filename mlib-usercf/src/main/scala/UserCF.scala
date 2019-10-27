

import breeze.numerics.{pow, sqrt}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * create by 2019-10-27 zq
  */
object UserCF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sparkSession = SparkSession.builder()
      // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
      .config(sparkConf)
      .config("hive.metastore.uris", "thrift://172.16.18.18:9083")
      //      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://72.16.18.18:9990/user/hive/warehouse")
      .config("spark.testing.memory", "2147480000")
      .enableHiveSupport().getOrCreate()


    val sql = "select * from hive.udata"
    import sparkSession.implicits._
    val userDataRDD = sparkSession.sql(sql).as[UserItem].rdd


    // 1、计算相似用户，使用cosine = a*b/(|a|*|b|)
    //1.1、计算分母 格式：（user_id, rating）
    val userScoreSumRDD = userDataRDD.map(x => (x.user_id.toString, x.rating))
      .groupByKey().mapValues(v => sqrt(v.toArray.map(rating=> pow(rating, 2)).sum))


    //1.2、倒排表(基于item的笛卡儿积)
    val vDataDF =  userDataRDD.toDF().selectExpr("user_id as user_v", "item_id", "rating as rating_v")

    vDataDF.foreach(println(_))



    //关闭sparkSession
    sparkSession.close()

  }


  /**
    * 生成数据
    *
    * @param sparkSession
    */
  def mockData(sparkSession: SparkSession): Unit = {
    //数据行  500个用户 1000种商品
    val rows = new ArrayBuffer[UserItem]()

    //定义评分5级评分
    val scoreArr = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    //购买次数组，用于随机购买
    val buyArr = Array(10, 20, 30, 40, 50, 60)

    val random = new Random()
    for (i <- 1 to 500) {
      //随机指定购买次数
      val buyCount = random.nextInt(buyArr.length)
      for (j <- 1 to buyArr(buyCount)) {
        //随机指定购买评分
        rows += UserItem(i, random.nextInt(1000) + 1, scoreArr(random.nextInt(scoreArr.length)), null)
      }
    }
    import sparkSession.implicits._
    val rowsRDD = sparkSession.sparkContext.makeRDD(rows).toDF()
    val tableName = "hive.udata"
    insertHive(sparkSession, tableName, rowsRDD)
  }

  /**
    * 插入数据到hive
    *
    * @param sparkSession
    * @param tableName
    * @param dataDF
    */
  def insertHive(sparkSession: SparkSession, tableName: String, dataDF: DataFrame) = {
    val sql = "drop table if exists " + tableName;
    sparkSession.sql(sql)
    dataDF.write.saveAsTable(tableName)
  }

}


case class UserItem(user_id: Long, item_id: Long, rating: Double, timestamp1: String)


