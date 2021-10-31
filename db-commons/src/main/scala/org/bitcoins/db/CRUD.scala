package org.bitcoins.db

import grizzled.slf4j.Logging
import org.bitcoins.core.util.TaskUtil
import slick.dbio.{DBIOAction, NoStream}
import slick.lifted.AbstractTable
import zio.{Task, ZIO}

import java.sql.SQLException
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/** Created by chris on 9/8/16.
  * This is an abstract actor that can be used to implement any sort of
  * actor that accesses a Postgres database. It creates
  * read, update, upsert, and delete methods for your actor to call.
  * You are responsible for the create function. You also need to specify
  * the table and the database you are connecting to.
  */
abstract class CRUD[T, PrimaryKeyType](implicit
    private val ec: ExecutionContext,
    override val appConfig: DbAppConfig)
    extends JdbcProfileComponent[DbAppConfig] {

  import profile.api._
  import scala.language.implicitConversions

  val schemaName: Option[String] = appConfig.schemaName

  /** We need to cast from TableQuery's of internal types (e.g. AddressDAO#AddressTable) to external
    * versions of them (e.g. AddressDAO().table). You'll notice that although the latter is a subtype
    * of the first, this requires a cast since TableQuery is not covariant in its type parameter.
    *
    * However, since Query is covariant in its first type parameter, I believe the cast from
    * TableQuery[T1] to TableQuery[T2] will always be safe so long as T1 is a subtype of T2
    * AND T1#TableElementType is equal to T2#TableElementType.
    *
    * The above conditions are always the case when this is called within DAOs as it is only
    * ever used for things of the form TableQuery[XDAO().table] -> TableQuery[XDAO#XTable].
    */
  implicit protected def tableQuerySafeSubtypeCast[
      SpecificT <: AbstractTable[_],
      SomeT <: SpecificT](
      tableQuery: TableQuery[SomeT]): TableQuery[SpecificT] = {
    tableQuery.asInstanceOf[TableQuery[SpecificT]]
  }

  /** The table inside our database we are inserting into */
  val table: profile.api.TableQuery[_ <: profile.api.Table[T]]

  /** Binding to the actual database itself, this is what is used to run querys */
  def safeDatabase: SafeDatabase = SafeDatabase(this)

  /** create a record in the database
    *
    * @param t - the record to be inserted
    * @return the inserted record
    */
  def create(t: T): Task[T] = {
    logger.trace(s"Writing $t to DB with config: $appConfig")
    createAll(Vector(t)).map(_.head)
  }

  def createAll(ts: Vector[T]): Task[Vector[T]]

  /** read a record from the database
    *
    * @param id - the id of the record to be read
    * @return Option[T] - the record if found, else none
    */
  def read(id: PrimaryKeyType): Task[Option[T]] = {
    logger.trace(s"Reading from DB with config: ${appConfig}")
    val query = findByPrimaryKey(id)
    safeDatabase.run(query.result).map(_.headOption)
  }

  /** Update the corresponding record in the database */
  def update(t: T): Task[T] = updateAll(Vector(t)).map {
    _.headOption match {
      case None          => throw UpdateFailedException("Update failed for: " + t)
      case Some(updated) => updated
    }
  }

  def updateAll(ts: Vector[T]): Task[Vector[T]] =
    ZIO.ifM(ZIO.succeed(ts.isEmpty))(
      onTrue = Task.succeed(ts),
      onFalse = for {
        actions <- Task.succeed(ts.map(t => find(t).update(t)))
        numUpdated <- safeDatabase.runVec(
          DBIO.sequence(actions).transactionally)
        _ <- ZIO.unless(numUpdated.sum == ts.length) {
          Task.fail(new RuntimeException(
            s"Unexpected number of updates completed ${numUpdated.sum} of ${ts.length}"))
        }
        tsUpdated <- Task.succeed(ts)
      } yield tsUpdated
    )

  /** delete the corresponding record in the database
    *
    * @param t - the record to be deleted
    * @return int - the number of rows affected by the deletion
    */
  def delete(t: T): Task[Int] = {
    logger.debug("Deleting record: " + t)
    safeDatabase.run(find(t).delete)
  }

  def deleteAll(ts: Vector[T]): Task[Int] = safeDatabase.run(findAll(ts).delete)

  /** delete all records from the table
    */
  def deleteAll(): Task[Int] = safeDatabase.run(table.delete.transactionally)

  /** insert the record if it does not exist, update it if it does
    *
    * @param t - the record to inserted / updated
    * @return t - the record that has been inserted / updated
    */
  def upsert(t: T): Task[T] = upsertAll(Vector(t)).flatMap {
    _.headOption match {
      case None          => Task.fail(UpsertFailedException("Upsert failed for: " + t))
      case Some(updated) => Task.succeed(updated)
    }
  }

  /** Upserts all of the given ts in the database, then returns the upserted values */
  def upsertAll(ts: Vector[T]): Task[Vector[T]] =
    TaskUtil.foldLeftAsync(Vector.empty[T], ts) { (accum, t) =>
      lazy val transaction =
        DBIO.sequence(Vector(t).map(table.insertOrUpdate)).transactionally
      safeDatabase.run(transaction) *> safeDatabase
        .runVec(findAll(accum).result)
        .map(accum ++ _)
    }

  /** return all rows that have a certain primary key
    *
    * @param id
    * @return Query object corresponding to the selected rows
    */
  protected def findByPrimaryKey(id: PrimaryKeyType): Query[Table[_], T, Seq] =
    findByPrimaryKeys(Vector(id))

  /** Finds the rows that correlate to the given primary keys */
  protected def findByPrimaryKeys(
      ids: Vector[PrimaryKeyType]): Query[Table[T], T, Seq]

  /** return the row that corresponds with this record
    *
    * @param t - the row to find
    * @return query - the sql query to find this record
    */
  protected def find(t: T): Query[Table[_], T, Seq] = findAll(Vector(t))

  protected def findAll(ts: Vector[T]): Query[Table[_], T, Seq]

  /** Finds all elements in the table */
  def findAll(): Task[Vector[T]] =
    safeDatabase.run(table.result).map(_.toVector)

  /** Returns number of rows in the table */
  def count(): Task[Int] = safeDatabase.run(table.length.result)
}

final case class SafeDatabase(jdbcProfile: JdbcProfileComponent[DbAppConfig])
    extends Logging {

  import jdbcProfile.database
  import jdbcProfile.profile.api.actionBasedSQLInterpolation

  /** SQLite does not enable foreign keys by default. This query is
    * used to enable it. It must be included in all connections to
    * the database.
    */
  private val foreignKeysPragma = sqlu"PRAGMA foreign_keys = TRUE;"
  private val sqlite = jdbcProfile.appConfig.driver == DatabaseDriver.SQLite

  /** Logs the given action and error, if we are not on mainnet */
  private def logAndThrowError(
      action: DBIOAction[_, NoStream, _]): PartialFunction[
    Throwable,
    Nothing
  ] = { case err: SQLException =>
    logger.error(
      s"Error when executing query ${action.getDumpInfo.getNamePlusMainInfo}")
    logger.error(s"$err")
    throw err
  }

  /** Runs the given DB action */
  def run[R](action: DBIOAction[R, NoStream, _]): Task[R] =
    ZIO.ifM(ZIO.succeed(sqlite))(
      Task.fromFuture(implicit ec =>
        database.run[R](foreignKeysPragma >> action).recoverWith {
          logAndThrowError(action)
        }),
      Task.fromFuture(implicit ec =>
        database.run[R](action).recoverWith { logAndThrowError(action) })
    )

  /** Runs the given DB sequence-returning DB action
    * and converts the result to a vector
    */
  def runVec[R](action: DBIOAction[Seq[R], NoStream, _]): Task[Vector[R]] =
    Task.fromFuture { implicit ec =>
      scala.concurrent
        .blocking {
          if (sqlite) database.run[Seq[R]](foreignKeysPragma >> action)
          else database.run[Seq[R]](action)
        }
        .map(_.toVector)
        .recoverWith { logAndThrowError(action) }
    }
}

final case class UpdateFailedException(message: String)
    extends RuntimeException(message)

final case class UpsertFailedException(message: String)
    extends RuntimeException(message)
