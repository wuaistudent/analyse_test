package datasource

import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import parser.LJBuilder


/**
  * Created by Administrator on 2018/6/13.
  */
abstract class LJAbstractSqlParser extends ParserInterface {

  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression())
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText){ parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }



  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText),"Unsupported SQL statement", position, position)
    }

  }

  protected def astBuilder: LJBuilder

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    println(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)

    parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
    toResult(parser)
  }

}
