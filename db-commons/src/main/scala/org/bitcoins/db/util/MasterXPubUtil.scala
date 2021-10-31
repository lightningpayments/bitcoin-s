package org.bitcoins.db.util

import org.bitcoins.core.crypto.ExtPublicKey
import org.bitcoins.db.models.MasterXPubDAO
import zio.Task

object MasterXPubUtil {

  /** Checks our database xpub is the same as our key manager xpub
    * @throws RuntimeExceptions if the xpubs do not match
    */
  def checkMasterXPub(xpub: ExtPublicKey, masterXPubDAO: MasterXPubDAO): Task[Boolean] =
    masterXPubDAO.validate(xpub).map(_ => true)
}
