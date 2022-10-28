package spark.training.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties



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
    // 指定mysql连接地址
    val url = "jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8"
    // 指定要加载的表名
    val student = "students"
    val score = "scores"
    // 配置连接数据库的相关属性
    val properties = new Properties()
    // 用户名
    properties.setProperty("user", "root")
    // 密码
    properties.setProperty("password", "123456")

    val studentFrame: DataFrame = spark.read.jdbc(url, student, properties)
    val scoreFrame: DataFrame = spark.read.jdbc(url, score, properties)
    // 把dataFrame注册成表
    studentFrame.createTempView("student")
    scoreFrame.createOrReplaceTempView("score")
    // spark.sql("SELECT temp1.class,SUM(temp1.degree),AVG(temp1.degree) FROM (SELECT  students.sno AS ssno,
    // students.sname,students.ssex,students.sbirthday,students.class, scores.sno,scores.degree,scores.cno
    // FROM students LEFT JOIN scores ON students.sno =  scores.sno ) temp1  GROUP BY temp1.class; ").show()
    val resultFrame: DataFrame = spark.sql("SELECT temp1.class,SUM(temp1.degree),AVG(temp1.degree)  " +
      "FROM (SELECT  students.sno AS ssno,students.sname,students.ssex,students.sbirthday,students.class, " +
      "scores.sno,scores.degree,scores.cno  FROM students LEFT JOIN scores ON students.sno =  scores.sno  " +
      "WHERE degree > 60 AND sbirthday > '1973-01-01 00:00:00' ) temp1 GROUP BY temp1.class")
    resultFrame.explain(true)
    resultFrame.show()
    spark.stop()
  }
}
