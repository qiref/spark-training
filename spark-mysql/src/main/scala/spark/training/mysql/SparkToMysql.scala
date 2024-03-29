package spark.training.mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * author YaoQi
  * Date: 2019/4/30 15:04
  * Description: spark-MySQL读写数据
  */
object SparkToMysql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-mysql").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val readConnectionProperties = dbConnectionProperties("root", "root")
    //    dataFrame.createOrReplaceGlobalTempView("")
    val sql =
      """( SELECT
        | tb_student.id,
        | tb_student.name,
        | tb_student.age,
        | tb_student.describe,
        | tb_stu_class.class_id
        | FROM
        | tb_student
        | LEFT JOIN tb_stu_class ON tb_stu_class.stu_id = tb_student.id ) AS tb
      """.stripMargin
    val dataFrame = sparkSession.read.jdbc(getJdbcUrl("127.0.0.1", "3306", "test"), sql, readConnectionProperties)
    //dataFrame.show(10)

    import sparkSession.implicits._
    // 查看dataFrame基本信息
    dataFrame.printSchema()
    dataFrame.select("id").show(10)
    dataFrame.select($"name", $"age" + 1, $"describe").show()

    val newFrame = dataFrame.select($"name", $"age" + 1, $"describe", $"class_id")
    // 此处创建的是全局的临时视图，因此在获取时需要使用 global_temp.tempFrame 才能找到该视图
    newFrame.createOrReplaceGlobalTempView("tempFrame")
    val sqlDF = sparkSession.sql("select * from global_temp.tempFrame")
    sqlDF.show()
  }

  def getJdbcUrl(ip: String, port: String, database: String): String = {
    val jdbcUrl = s"jdbc:mysql://$ip:$port/$database?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    jdbcUrl
  }

  def dbConnectionProperties(user: String, password: String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("fetchsize", "1000")
    connectionProperties.put("batchsize", "10000")
    connectionProperties
  }
}
