package parser

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, ListQuery, Literal, Not, RLike}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
//import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias, UnresolvedAttribute, UnresolvedGenerator, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LogicalPlan, OneRowRelation}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Administrator on 2018/6/13.
  */
class LJBuilder extends SqlBaseBaseVisitor[AnyRef]{
  import ParserUtils._

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitChildren(node: RuleNode): AnyRef = {
    if(node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier), None)
  }

  //不用sample
  override def visitTableName(ctx: SqlBaseParser.TableNameContext): LogicalPlan = withOrigin(ctx) {
    val table = UnresolvedRelation(
      visitTableIdentifier(ctx.tableIdentifier),
        Option(ctx.strictIdentifier).map(_.getText))
    table
  }

  override def visitSingleInsertQuery(ctx: SqlBaseParser.SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryTerm())
  }

  override def visitTableIdentifier(ctx: SqlBaseParser.TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
   TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  override def visitSingleExpression(ctx: SqlBaseParser.SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression())
  }

  override def visitSingleTableIdentifier(ctx: SqlBaseParser.SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    visitTableIdentifier(ctx.tableIdentifier())
  }

  override def visitNamedExpression(ctx: SqlBaseParser.NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if(ctx.identifier != null) {
      //Alias(e, ctx.identifier().getText)
      //瞬间不知道怎么改这个东西
      e
    } else if (ctx.identifierList() != null ) {
      MultiAlias(e, visitIdentifierList(ctx.identifierList))
    } else {
      e
    }
  }

  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }
  /* ********************************************************************************************
 * Expression parsing
 * ******************************************************************************************** */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /* ********************************************************************************************
 * Plan parsing
 * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  override def visitQuery(ctx: SqlBaseParser.QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryNoWith)
    query
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation.optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withQuerySpecification(ctx, from)
  }

  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val right = plan(relation.relationPrimary)
      val join = right.optionalMap(left)(Join(_, _, Inner, None))
      withJoinRelations(join, relation)
    }
    ctx.lateralView.asScala.foldLeft(from)(withGenerate)
  }
  private def withJoinRelations(base: LogicalPlan, ctx: SqlBaseParser.RelationContext): LogicalPlan = {
    ctx.joinRelation.asScala.foldLeft(base) {(left, join) =>
      withOrigin(join) {
        val baseJoinType = join.joinType match {
          case null => Inner
          case jt if jt.RIGHT != null => RightOuter
          case jt if jt.LEFT != null => LeftOuter
          case _ => Inner
        }

        val (joinType, condition) = Option(join.joinCriteria) match {
          case None => (baseJoinType, None)
        }
        Join(left, plan(join.right), joinType, condition)
      }
    }
  }

  //简化版试一下
  private def withQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext, relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      Filter(expression(ctx), plan)
    }

    val expressions = Option(namedExpressionSeq()).toSeq
      .flatMap(_.namedExpression().asScala)
      .map(typedVisit[Expression])

    val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
    specType match {
      case SqlBaseParser.SELECT =>
        val withLateralView = ctx.lateralView().asScala.foldLeft(relation)(withGenerate)
        val withFilter = withLateralView.optionalMap(where)(filter)

        withFilter
    }
  }
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression)
  }

  protected def visitFunctionName(ctx: SqlBaseParser.QualifiedNameContext): FunctionIdentifier = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}", ctx)
    }
  }

  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ | SqlBaseParser.NEQJ =>
        Not(EqualTo(left, right))
      case SqlBaseParser.LT =>
        LessThan(left, right)
      case SqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case SqlBaseParser.GT =>
        GreaterThan(left, right)
      case SqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case SqlBaseParser.BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case SqlBaseParser.IN if ctx.query != null =>
        invertIfNotDefined(In(e, Seq(ListQuery(plan(ctx.query)))))
      case SqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression)))
      case SqlBaseParser.LIKE =>
        invertIfNotDefined(Like(e, expression(ctx.pattern)))
      case SqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case SqlBaseParser.NULL if ctx.NOT != null =>
        IsNotNull(e)
      case SqlBaseParser.NULL =>
        IsNull(e)
    }
  }

  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case UnresolvedAttribute(nameParts) =>
        UnresolvedAttribute(nameParts :+ attr)
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(ctx.getText)
  }



  private def withGenerate(query: LogicalPlan, ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      join = true,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }


}
