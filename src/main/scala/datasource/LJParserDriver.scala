package datasource

import org.antlr.v4.runtime.{ANTLRInputStream, IntStream, ParserRuleContext}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.trees.Origin

/**
  * Created by Administrator on 2018/6/13.
  */
class LJParserDriver {

}


class ANTLRNoCaseStringStream(input: String) extends ANTLRInputStream(input) {
  override def LA(i: Int): Int = {
    val la = super.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

