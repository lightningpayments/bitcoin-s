package org.bitcoins.core.util

import zio.{Task, ZIO}

object TaskUtil {

  implicit class RichOption[A](option: Option[A]) {

    def toTask(throwable: Throwable): Task[A] = option match {
      case Some(a) => Task.succeed(a)
      case None    => Task.fail(throwable)
    }
  }

  val unit: Task[Unit] = Task.unit

  def foldLeftAsync[T, U](init: T, items: Seq[U])(fun: (T, U) => Task[T]): Task[T] =
    items.foldLeft(ZIO(init)) { case (accumF, elem) => accumF.flatMap(fun(_, elem)) }

}
