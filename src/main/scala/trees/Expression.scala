package trees

/**
  * Created by Administrator on 2018/6/20.
  */
abstract class Expression extends TreeNode[Expression]{
  //能否展开
  def foldable: Boolean = false

}
