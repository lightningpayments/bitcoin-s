package org.bitcoins.db

import cats.implicits.toFunctorOps
import grizzled.slf4j.Logging
import org.bitcoins.core.util.TaskUtil
import org.bitcoins.db.DatabaseDriver._
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.{FlywayException, MigrationInfoService}
import slick.dbio.Effect
import slick.dbio.Effect.Schema
import slick.sql.FixedSqlAction
import zio.Task

import scala.concurrent.{ExecutionContext, Future}

trait DbManagement extends Logging {
  _: JdbcProfileComponent[DbAppConfig] =>
  import profile.api._

  import scala.language.implicitConversions

  private lazy val flyway: Flyway = {
    val module = appConfig.moduleName

    val driverName = appConfig.driver match {
      case SQLite     => "sqlite"
      case PostgreSQL => "postgresql"
    }

    val config = {
      val conf = Flyway
        .configure()
        .locations(s"classpath:$driverName/$module/migration/")
      appConfig.schemaName match {
        case Some(schema) =>
          conf
            .schemas(schema)
            .defaultSchema(schema)
        case None => conf
      }
    }

    // Remove "s needed for config
    val url = appConfig.jdbcUrl.replace("\"", "")
    config
      .dataSource(url, username, password)
      .load
  }

  /** Internally, slick defines the schema member as
    *
    * def schema: SchemaDescription = buildTableSchemaDescription(q.shaped.value.asInstanceOf[Table[_]])
    *
    * we need to cast between TableQuery's of specific table types to the more generic TableQuery[Table[_]]
    * to get methods in this trait working as they require schema (which essentially does this cast anyway)
    *
    * This cast is needed because TableQuery is not covariant in its type parameter. However, since Query
    * is covariant in its first type parameter, I believe the cast from TableQuery[T1] to TableQuery[T2] will
    * always be safe so long as T1 is a subtype of T2 AND T1#TableElementType is equal to T2#TableElementType.
    *
    * The above conditions are always the case when this is called in the current code base and will
    * stay that way so long as no one tries anything too fancy.
    */
  implicit protected def tableQueryToWithSchema(tableQuery: TableQuery[_]): TableQuery[Table[_]] = {
    tableQuery.asInstanceOf[TableQuery[Table[_]]]
  }

  def allTables: List[TableQuery[Table[_]]]

  def dropAll(): Task[Unit] =
    TaskUtil
      .foldLeftAsync((), allTables.reverse)((_, table) => dropTable(table))
      .foldM(e => Task(e.printStackTrace()) *> Task.unit, _ => Task.unit)

  /** The query needed to create the given table */
  private def createTableQuery(
      table: TableQuery[_ <: Table[_]],
      createIfNotExists: Boolean
  ): FixedSqlAction[Unit, NoStream, Schema] =
    if (createIfNotExists) table.schema.createIfNotExists else table.schema.create

  /** Creates the given table */
  def createTable(table: TableQuery[_ <: Table[_]], createIfNotExists: Boolean = true)(implicit
      ec: ExecutionContext): Future[Unit] = {
    val tableName = table.baseTableRow.tableName
    logger.debug(s"Creating table $tableName with DB config: $appConfig")

    val query = createTableQuery(table, createIfNotExists)
    database.run(query).map(_ => logger.debug(s"Created table $tableName"))
  }

  def dropTable(
      table: TableQuery[Table[_]]
  ): Task[Unit] = {
    val query = table.schema.dropIfExists
    Task.fromFuture(_ => database.run(query))
  }

  def dropTable(tableName: String): Task[Int] = {
    val fullTableName =
      appConfig.schemaName.map(_ + ".").getOrElse("") + tableName
    val sql = sqlu"""DROP TABLE IF EXISTS #$fullTableName"""

    Task.fromFuture { implicit ec =>
      lazy val future = database.run(sql)
      future.failed.foreach(_.printStackTrace())
      future
    }
  }

  def createSchema(createIfNotExists: Boolean = true): Task[Unit] =
    appConfig.schemaName match {
      case None => Task.unit
      case Some(schema) =>
        val sql =
          if (createIfNotExists) sqlu"""CREATE SCHEMA IF NOT EXISTS #$schema"""
          else sqlu"""CREATE SCHEMA #$schema"""
        Task.fromFuture(implicit ec => database.run(sql).as(()))
    }

  /** Returns flyway information about the state of migrations
    * @see https://flywaydb.org/documentation/command/info
    */
  def info(): MigrationInfoService = {
    flyway.info()
  }

  def migrationsApplied(): Int = {
    val applied = flyway.info().applied()
    appConfig.driver match {
      case SQLite =>
        applied.size
      case PostgreSQL =>
        // -1 because of extra << Flyway Schema Creation >>
        applied.size - 1
    }
  }

  /** Executes migrations related to this database
    *
    * @see [[https://flywaydb.org/documentation/api/#programmatic-configuration-java]]
    */
  def migrate(): Int = {
    try {
      appConfig.driver match {
        case SQLite =>
          SQLiteUtil.createDbFileIfDNE(appConfig.dbPath, appConfig.dbName)
          val jdbcUrl = appConfig.jdbcUrl.replace("\"", "")
          SQLiteUtil.setJournalMode(jdbcUrl, "WAL")
        case PostgreSQL =>
          ()
      }
      flyway.migrate()
    } catch {
      case err: FlywayException =>
        logger.warn(s"Failed to apply first round of migrations, attempting baseline and re-apply", err)
        //maybe we have an existing database, so attempt to baseline the existing
        //database and then apply migrations again
        flyway.baseline()
        flyway.migrate()
    }
  }

  /** Runs flyway clean
    *
    * WARNING:
    * THIS DELETES ALL DATA IN THE DATABASE, YOU PROBABLY DON'T WANT THIS UNLESS YOU ARE USING TESTS
    *
    * @see https://flywaydb.org/documentation/command/clean
    */
  private[bitcoins] def clean(): Unit = {
    flyway.clean()
  }
}
