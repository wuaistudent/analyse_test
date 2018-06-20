package test

import datasource.LJSqlParser
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by Administrator on 2018/6/13.
  */
object DfTest {

  def main(args: Array[String]): Unit = {

    val parse = new LJSqlParser
    val sql = s"select * from test where test.id = 1"
    val res = parse.parsePlan(sql)
    println(res)

  }

}
