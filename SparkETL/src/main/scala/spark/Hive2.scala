package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties
import scala.util.matching.Regex

/*
  1.spark版本变更为2.3.3，部署模式local即可。也可探索其他模式。
  2.由于远程调试出现的各种问题，且远程调试并非作业重点，这里重新建议使用spark-submit方式
  3.本代码及spark命令均为最简单配置。如运行出现资源问题，请根据你的机器情况调整conf的配置以及spark-submit的参数，具体指分配CPU核数和分配内存。

  调试：
    当前代码中集成了spark-sql，可在开发机如windows运行调试;
    需要在开发机本地下载hadoop，因为hadoop基于Linux编写，在开发机本地调试需要其中的一些文件，如模拟Linux目录系统的winutils.exe；
    请修改System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")

  部署：
    注释掉System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")；
    修改pom.xml中<scope.mode>compile</scope.mode>为<scope.mode>provided</scope.mode>
    打包 mvn clean package
    上传到你的Linux机器

    注意在~/bash_profile文件中配置$SPARK_HOME,并source ~/bash_profile,或在bin目录下启动spark-submit
    spark-submit Spark2DB-1.0.jar
 */


object Hive2 {
    // parameters
    LoggerUtil.setSparkLogLevels()

    // 定义电话号码匹配的正则表达式
    val phoneRegex: Regex = "^1[3-9]\\d{9}$|\\d{7,8}$".r

    def main(args: Array[String]): Unit = {
        // System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-2.7.4")

        val conf = new SparkConf()
            .setAppName(this.getClass.getSimpleName)
            .setMaster("local[*]")

        val session = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        val reader = session.read.format("jdbc")
            .option("url", "jdbc:hive2://36.134.119.108:31000/default")
            .option("user", "student")
            .option("password", "nju2024")
            .option("driver", "org.apache.hive.jdbc.HiveDriver")
        val registerHiveDqlDialect = new RegisterHiveSqlDialect()
        registerHiveDqlDialect.register()

        val tblNameDsts = List("dm_v_as_djk_info")


        for (tblNameDst <- tblNameDsts) {
            var df = reader.option("dbtable", tblNameDst).load()
            val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
            df = df.toDF(columnNames: _*)

            // code
            System.out.println(df.toString())
            df.dropDuplicates()
            df.na.drop()

//            // 应用UDF到DataFrame并去除无用数据行
//            df = df.withColumn("contact", validatePhoneUDF(col("contact"), col("con_type")))
//              .where(col("contact") =!= "" && col("contact") =!= "无"
//                && col("contact") =!= "-" && col("contact").isNotNull)
//
//            // 在数据清洗完成后，继续进行数据聚合操作
//            var groupedDF = df
//              .withColumn("contact_phone", when(col("con_type").isin("TEL", "OTH", "MOB"), col("contact")).otherwise(lit(null)))
//              .withColumn("contact_address", when(col("con_type").isin("TEL", "OTH", "MOB"), lit(null)).otherwise(col("contact")))
//              .groupBy("uid")
//              .agg(
//                  concat_ws(",", collect_set("contact_phone")).alias("contact_phone"),
//                  concat_ws(",", collect_set("contact_address")).alias("contact_address")
//              )
//              .withColumn("contact_phone", when(col("contact_phone") =!= "", col("contact_phone")).otherwise(lit(null)))
//              .withColumn("contact_address", when(col("contact_address") =!= "", col("contact_address")).otherwise(lit(null)))
//            // 对于数据再次清洗，确保phone和address都有数据
//            groupedDF = groupedDF.filter(
//                (col("contact_phone") =!= "" && col("contact_address") =!= "")
//            )
//            // 输出最终结果
//            groupedDF.show()
            // 写入clickhouse
            // 设置 ClickHouse 连接参数
            val clickHouseProperties = new Properties()
            clickHouseProperties.put("user", "default")
            clickHouseProperties.put("password", "123456")
            clickHouseProperties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            df.write
              .mode(SaveMode.Append)
              .jdbc("jdbc:clickhouse://localhost:8123/dm", "dm.dm_v_as_djk_info",
                  clickHouseProperties)


        }
        session.close()
    }

//    // 定义UDF
//    val validatePhoneUDF: UserDefinedFunction = udf((phone: String, con_type: String) => {
//        // 检查con_type是否是 "TEL"、"OTH" 或 "MOB"
//        if (Set("TEL", "OTH", "MOB").contains(con_type)) {
//            // 如果是，检查phone是否匹配正则表达式
//            if (phoneRegex.findFirstIn(phone).isDefined) {
//                phone // 如果是匹配的格式，返回phone
//            } else {
//                "" // 如果不匹配，返回空字符串
//            }
//        } else {
//            phone // 如果con_type不是 "TEL"、"OTH" 或 "MOB"，直接返回phone
//        }
//    })
}
