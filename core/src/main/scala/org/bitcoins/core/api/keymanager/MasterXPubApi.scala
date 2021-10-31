package org.bitcoins.core.api.keymanager

import zio.Task

trait MasterXPubApi {

  /** Determines if the seed exists */
  def seedExists(): Task[Boolean]
}
