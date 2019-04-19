package utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

/**
  * @ClassName PhoenixDemo
  * @Description spark连接phoenix的样例代码
  * @Author HuZhongJin
  * @Date 2019/4/19 9:15
  * @Version 1.0
  */
object PhoenixDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val conf = new Configuration
    conf.addResource("phoenix/core-site.xml")
    conf.addResource("phoenix/hbase-site.xml")
    conf.addResource("phoenix/hdfs-site.xml")
    
    val tableName = "test"

    val df = spark.sqlContext.phoenixTableAsDataFrame(
      "ENBRANDS_1000834.CROWD_INSTANCE_6000000126",
      Seq("USER_ID"),
      conf = conf,
      zkUrl = Some("zk1,zk2,zk3:2181:/hbase")
    )
    println(df.first())
  }
}
