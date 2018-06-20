package trees

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


case class Origin(line: Option[Int] = None, startPosition: Option[Int] = None)
//which line is being parsed
object CurrentOrigin{

  private val value = new ThreadLocal[Origin]() {
    override def initialValue(): Origin = Origin()
  }
  def get: Origin = value.get()

  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int) = {
    value.set(value.get().copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    reset()
    ret
  }

}
/**
  * Created by Administrator on 2018/6/20.
  */
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product  {

  self: BaseType =>
  val origin: Origin = CurrentOrigin.get

  def children: Seq[BaseType]
  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  //这么写挺有意思的
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f))}
  }

  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  def map[A](f: BaseType => A): Seq[A] = {
    val ret = ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret += ))
    ret
  }

  def collectLeaves(): Seq[BaseType] = {
    this.collect{ case p if p.children.isEmpty => p}
  }

  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse{
      children.foldLeft(Option.empty[B]){ (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while ( i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

}
