package datasource

import org.apache.spark.sql.catalyst.parser.{AstBuilder, SqlBaseParser}
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.SQLConf
import parser.LJBuilder

/**
  * Created by Administrator on 2018/6/13.
  */
class LJSqlParser extends LJAbstractSqlParser{

  val astBuilder = new LJBuilder

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(command)(toResult)
  }

}
