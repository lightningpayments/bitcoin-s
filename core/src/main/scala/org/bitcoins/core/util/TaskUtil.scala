package org.bitcoins.core.util

import zio.{Task, ZIO}

object TaskUtil {

  val unit: Task[Unit] = Task.unit

  def foldLeftAsync[T, U](init: T, items: Seq[U])(fun: (T, U) => Task[T]): Task[T] =
    items.foldLeft(ZIO(init)) { case (accumF, elem) =>
      accumF.flatMap { accum =>
        fun(accum, elem)
      }
    }

}
