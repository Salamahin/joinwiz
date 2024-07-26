package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.whitebox


trait TWindow[O, E] {
  def partitionByCols: List[Column]
  def orderByCols: List[Column]
  def ordering: Option[Ordering[O]]

  def apply(): WindowSpec = Window.partitionBy(partitionByCols: _*).orderBy(orderByCols: _*)
  def apply(o: O): E

  def partitionBy[S](expr: O => S): TWindow[O, (E, S)] = macro MacroImpl.partitionWindowBy[O, E, S]
  def orderByAsc[S](expr: O => S): TWindow[O, E] = macro MacroImpl.orderWindowByAsc[O, E, S]
  def orderByDesc[S](expr: O => S): TWindow[O, E] = macro MacroImpl.orderWindowByDesc[O, E, S]
}

class ApplyTWindow[O] extends Serializable {
  def partitionBy[S](expr: O => S): TWindow[O, S] = macro MacroImpl.basicTWindow[O, S]
}

object TWindow {
  def composeOrdering[T](first: Ordering[T], second: Ordering[T]): Ordering[T] = new Ordering[T] {
    override def compare(x: T, y: T): Int = {
      val compared = first.compare(x, y)
      if (compared == 0) second.compare(x, y)
      else compared
    }
  }

  def composeOrdering[T](maybePrevOrdering: Option[Ordering[T]], nextOrdering: Ordering[T]): Option[Ordering[T]] = maybePrevOrdering match {
    case None       => Some(nextOrdering)
    case Some(prev) => Some(composeOrdering(prev, nextOrdering))
  }
}

private object MacroImpl {

  def basicTWindow[O: c.WeakTypeTag, S: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, S]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, $fieldType] {
            import org.apache.spark.sql.functions.col
            override def partitionByCols = col($fieldName) :: Nil
            override def ordering = None
            override def orderByCols = Nil
            override def apply(o: $origType) = $expr(o)
         }
       """
    )
  }

  def partitionWindowBy[O: c.WeakTypeTag, E: c.WeakTypeTag, S: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, (E, S)]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val prevType  = c.weakTypeOf[E]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, ($prevType, $fieldType)] {
            private val prev = ${c.prefix}

            import org.apache.spark.sql.functions.col
            override def partitionByCols = prev.partitionByCols :+ col($fieldName)
            override def ordering = prev.ordering
            override def orderByCols = prev.orderByCols
            override def apply(o: $origType) = (prev(o), $expr(o))
         }
       """
    )
  }

  def orderWindowByAsc[O: c.WeakTypeTag, E: c.WeakTypeTag, S: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, E]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val prevType  = c.weakTypeOf[E]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, $prevType] {
            private val prev = ${c.prefix}

            import org.apache.spark.sql.functions.col
            override def partitionByCols = prev.partitionByCols
            override def ordering = joinwiz.TWindow.composeOrdering(prev.ordering, Ordering.by[$origType, $fieldType]($expr))
            override def orderByCols = prev.orderByCols :+ col($fieldName)
            override def apply(o: $origType) = prev(o)
         }
       """
    )
  }

  def orderWindowByDesc[O: c.WeakTypeTag, E: c.WeakTypeTag, S: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, E]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val prevType  = c.weakTypeOf[E]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, $prevType] {
            private val prev = ${c.prefix}

            import org.apache.spark.sql.functions.col
            override def partitionByCols = prev.partitionByCols
            override def ordering = joinwiz.TWindow.composeOrdering(prev.ordering, Ordering.by[$origType, $fieldType]($expr).reverse)
            override def orderByCols = prev.orderByCols :+ col($fieldName).desc
            override def apply(o: $origType) = prev(o)
         }
       """
    )
  }

  private def extractArgName[E: c.WeakTypeTag, T: c.WeakTypeTag](c: whitebox.Context)(func: c.Expr[E => T]): String = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree, acc: List[String]): List[String] = {
      tree match {
        case Ident(_)          => acc
        case Select(q, n)      => extract(q, n.decodedName.toString :: acc)
        case Function(_, body) => extract(body, acc)
        case _                 => c.abort(c.enclosingPosition, s"Unsupported expression: $func, apply should be used for products member selection only")
      }
    }

    extract(func.tree, Nil).mkString(".")
  }
}
