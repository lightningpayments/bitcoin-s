package org.bitcoins.db

import slick.jdbc.JdbcProfile
import zio.Task

trait SlickUtil[T, PrimaryKeyType] { _: CRUD[T, PrimaryKeyType] =>
  def profile: JdbcProfile

  import profile.api._

  /** Creates rows in a database that are not auto incremented */
  def createAllNoAutoInc(ts: Vector[T], database: SafeDatabase): Task[Vector[T]] = {
    lazy val actions = (table ++= ts).andThen(DBIO.successful(ts)).transactionally
    database.run(actions)
  }
}
