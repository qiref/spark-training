package spark.training.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def getJdbcUrl(ip: String, port: String, database: String): String = {
    val jdbcUrl = s"jdbc:mysql://$ip:$port/$database?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    jdbcUrl
  }

  def dbConnectionProperties(user: String, password: String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("fetchsize", "1000")
    connectionProperties.put("batchsize", "10000")
    connectionProperties
  }

  def main(args: Array[String]): Unit = {
    val student = "student"
    val score = "score"
    val session = getDefaultSparkSession()
    val studentFrame: DataFrame = session.read.jdbc(getJdbcUrl("9.135.222.17", "3306", "archie"),
      student,
      dbConnectionProperties("root", "QcloudV5!"))
    val scoreFrame: DataFrame = session.read.jdbc(getJdbcUrl("9.135.222.17", "3306", "archie"),
      score,
      dbConnectionProperties("root", "QcloudV5!"))
    // 把dataFrame注册成表
    studentFrame.createTempView("student")
    scoreFrame.createOrReplaceTempView("score")
    // spark.sql("SELECT temp1.class,SUM(temp1.degree),AVG(temp1.degree) FROM (SELECT  students.sno AS ssno,
    // students.sname,students.ssex,students.sbirthday,students.class, scores.sno,scores.degree,scores.cno
    // FROM students LEFT JOIN scores ON students.sno =  scores.sno ) temp1  GROUP BY temp1.class; ").show()
    val resultFrame: DataFrame = session.sql(
      "SELECT stu_score.class," +
        "      sum(stu_score.degree)," +
        "      avg(stu_score.degree)" +
        "FROM" +
        "  (SELECT student.sno AS ssno," +
        "         student.sname," +
        "          student.ssex," +
        "          student.sbirthday," +
        "          student.class," +
        "          score.sno," +
        "          score.degree," +
        "          score.cno" +
        "   FROM student" +
        "   LEFT JOIN score ON student.sno = score.sno) stu_score " +
        "GROUP BY stu_score.class")
    resultFrame.explain(true)
    resultFrame.show()
    session.stop()
  }
}
