package org.bitcoins.commons.util

import grizzled.slf4j.Logging
import zio.{Ref, Task, UIO, ZIO}

import java.io.File
import scala.sys.process.{Process, ProcessBuilder}

/** A trait that helps start bitcoind/eclair when it is started via bitcoin-s */
trait NativeProcessFactory extends Logging {

  private[this] val processOpt: UIO[Ref[Option[Process]]] = Ref.make(Option.empty[Process])

  private lazy val process: ProcessBuilder = scala.sys.process.Process(cmd)

  /** The command to start the daemon on the underlying OS */
  def cmd: String

  def isAlive: Task[Boolean] = processOpt.flatMap(_.get).map(_.fold(false)(_.isAlive()))

  /** Starts the binary by spinning up a new process */
  def startBinary(): Task[Unit] =
    processOpt.flatMap(_.get).flatMap {
      case Some(_) =>
        //don't do anything as it is already started
        Task(logger.debug(s"Binary was already started!"))
      case None =>
        if (cmd.nonEmpty) processOpt.map(_.set(Some(process.run()))).as(())
        else Task(logger.warn("cmd not set, no binary started"))
    }

  /** Stops the binary by destroying the underlying operating system process
    *
    * If the client is a remote client (not started on the host operating system)
    * this method is a no-op
    */
  def stopBinary(): Task[Unit] =
    processOpt.flatMap(_.get).flatMap {
      case Some(process) =>
        ZIO.when(process.isAlive())(Task(process.destroy())) *> processOpt.map(_.set(None)).as(())
      case None =>
        Task(logger.info(s"No process found, binary wasn't started!"))
    }

}

object NativeProcessFactory {

  def findExecutableOnPath(name: String): Option[File] =
    sys.env
      .getOrElse("PATH", "")
      .split(File.pathSeparator)
      .map(directory => new File(directory, name))
      .find(file => file.isFile && file.canExecute)

}
