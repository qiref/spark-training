package spark.training.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * author YaoQi
  * Date: 2020/4/28 14:57
  * Description:
  */
object DataFrameOperator {

  val APP_NAME_DEFAULT = "DATA_FRAME_OPERATOR"
  val MASTER_DEFAULT = "local[1]"

  /**
    * 获取sparkSession对象
    *
    * @param appName   应用名称
    * @param master    运行模式
    * @param sparkConf 配置信息
    */
  def getSparkSession(appName: String, master: String, sparkConf: SparkConf): SparkSession = {
    val builder = SparkSession.builder()
    if (appName == null || appName.isEmpty) {
      builder.appName(APP_NAME_DEFAULT)
    } else {
      builder.appName(appName)
    }
    if (master == null || master.isEmpty) {
      builder.master(MASTER_DEFAULT)
    } else {
      builder.master(master)
    }
    val sparkSession = builder.config(sparkConf).getOrCreate()
    sparkSession
  }

  /**
    * 获取一个默认的sparkSession
    *
    * @return
    */
  def getDefaultSparkSession(): SparkSession = {
    getSparkSession(APP_NAME_DEFAULT, MASTER_DEFAULT, new SparkConf())
  }

  def main(args: Array[String]): Unit = {

  }
}
