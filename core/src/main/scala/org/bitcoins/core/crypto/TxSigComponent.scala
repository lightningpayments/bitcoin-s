package org.bitcoins.core.crypto

import org.bitcoins.core.currency.CurrencyUnit
import org.bitcoins.core.number.UInt32
import org.bitcoins.core.protocol.script._
import org.bitcoins.core.protocol.transaction._
import org.bitcoins.core.script.flag.ScriptFlag

import scala.util.{Failure, Success, Try}

/**
 * Created by chris on 4/6/16.
 * Represents a transaction whose input is being checked against the spending conditions of a [[ScriptPubKey]]
 */
sealed abstract class TxSigComponent {

  /** The transaction being checked for the validity of signatures */
  def transaction: Transaction

  /** The index of the input whose script signature is being checked */
  def inputIndex: UInt32

  def input: TransactionInput = transaction.inputs(inputIndex.toInt)

  /** The script signature being checked */
  def scriptSignature: ScriptSignature = input.scriptSignature

  /** This is the output we are spending. We need this for script and digital signatures checks */
  def output: TransactionOutput

  /** The scriptPubKey for which the input is being checked against */
  def scriptPubKey: ScriptPubKey = output.scriptPubKey

  /** The flags that are needed to verify if the signature is correct */
  def flags: Seq[ScriptFlag]

  /** Represents the serialization algorithm used to verify/create signatures for Bitcoin */
  def sigVersion: SignatureVersion
}

/**
 * The [[TxSigComponent]] used to evaluate the the original Satoshi transaction digest algorithm.
 * Basically this is every spk that is not a [[WitnessScriptPubKey]] EXCEPT in the case of a
 * P2SH(witness script) [[ScriptPubKey]]
 */
sealed abstract class BaseTxSigComponent extends TxSigComponent {
  override def sigVersion = SigVersionBase

  override def scriptPubKey = output.scriptPubKey
}

/**
 * The [[TxSigComponent]] used to represent all the components necessarily for BIP143
 * [[https://github.com/bitcoin/bips/blob/master/bip-0143.mediawiki]]
 * Examples of these [[ScriptPubKey]]'s are [[P2WPKHWitnessSPKV0]],
 * [[P2WSHWitnessSPKV0]], and P2SH(witness script)
 */
sealed abstract class WitnessTxSigComponent extends TxSigComponent {

  override def transaction: WitnessTransaction

  def witness: ScriptWitness = transaction.witness.witnesses(inputIndex.toInt)

  /** The amount of [[CurrencyUnit]] this input is spending */
  def amount: CurrencyUnit

  def witnessVersion: WitnessVersion

  override def sigVersion = SigVersionWitnessV0
}

/** This represents checking the [[WitnessTransaction]] against a [[P2WPKHWitnessSPKV0]] or a [[P2WSHWitnessSPKV0]] */
sealed abstract class WitnessTxSigComponentRaw extends WitnessTxSigComponent {
  override def scriptPubKey: WitnessScriptPubKey = output.scriptPubKey.asInstanceOf[WitnessScriptPubKey]

  override def witnessVersion: WitnessVersion = {
    scriptPubKey.witnessVersion
  }

  override def amount: CurrencyUnit = output.value
}

/** This represents checking the [[WitnessTransaction]] against a P2SH(P2WSH) or P2SH(P2WPKH) scriptPubKey */
sealed abstract class WitnessTxSigComponentP2SH extends WitnessTxSigComponent {
  override def scriptPubKey: P2SHScriptPubKey = output.scriptPubKey.asInstanceOf[P2SHScriptPubKey]

  override def scriptSignature: P2SHScriptSignature = {
    val s = transaction.inputs(inputIndex.toInt).scriptSignature
    require(s.isInstanceOf[P2SHScriptSignature], "Must have P2SHScriptSignature for P2SH(P2WSH()), got: " + s)
    s.asInstanceOf[P2SHScriptSignature]
  }

  def witnessScriptPubKey: Try[WitnessScriptPubKey] = scriptSignature.redeemScript match {
    case w: WitnessScriptPubKey => Success(w)
    case x @ (_: P2PKScriptPubKey | _: P2PKHScriptPubKey | _: MultiSignatureScriptPubKey | _: P2SHScriptPubKey
      | _: CSVScriptPubKey | _: CLTVScriptPubKey | _: EscrowTimeoutScriptPubKey | _: NonStandardScriptPubKey
      | _: WitnessCommitment | EmptyScriptPubKey) =>
      Failure(new IllegalArgumentException("Must have a witness scriptPubKey as redeemScript for P2SHScriptPubKey in WitnessTxSigComponentP2SH, got: " + x))

  }

  override def witnessVersion: WitnessVersion = witnessScriptPubKey match {
    case Success(w) => w.witnessVersion
    case Failure(err) => throw err
  }

  override def amount = output.value

}

/**
 * This represents a 'rebuilt' [[ScriptPubKey]] that was constructed from [[WitnessScriptPubKey]]
 * After the [[ScriptPubKey]] is rebuilt, we need to use that rebuilt scriptpubkey to evaluate the [[ScriptSignature]]
 * See BIP141 for more info on rebuilding P2WSH and P2WPKH scriptpubkeys
 * [[https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki#witness-program]]
 */
sealed abstract class WitnessTxSigComponentRebuilt extends TxSigComponent {
  override def scriptPubKey: ScriptPubKey = output.scriptPubKey

  /** The [[WitnessScriptPubKey]] we used to rebuild the scriptPubKey above */
  def witnessScriptPubKey: WitnessScriptPubKey

  override def sigVersion = SigVersionWitnessV0

  def witnessVersion = witnessScriptPubKey.witnessVersion

  def amount: CurrencyUnit = output.value
}

object BaseTxSigComponent {

  private case class BaseTxSigComponentImpl(transaction: Transaction, inputIndex: UInt32,
    output: TransactionOutput, flags: Seq[ScriptFlag]) extends BaseTxSigComponent

  def apply(transaction: Transaction, inputIndex: UInt32,
    output: TransactionOutput, flags: Seq[ScriptFlag]): BaseTxSigComponent = {
    BaseTxSigComponentImpl(transaction, inputIndex, output, flags)
  }

}

object WitnessTxSigComponent {

  def apply(transaction: WitnessTransaction, inputIndex: UInt32, output: TransactionOutput,
    flags: Seq[ScriptFlag]): WitnessTxSigComponent = output.scriptPubKey match {
    case _: WitnessScriptPubKey => WitnessTxSigComponentRaw(transaction, inputIndex, output, flags)
    case _: P2SHScriptPubKey => WitnessTxSigComponentP2SH(transaction, inputIndex, output, flags)
    case x @ (_: P2PKScriptPubKey | _: P2PKHScriptPubKey | _: MultiSignatureScriptPubKey
      | _: LockTimeScriptPubKey | _: WitnessCommitment | _: NonStandardScriptPubKey
      | _: EscrowTimeoutScriptPubKey | EmptyScriptPubKey) =>
      throw new IllegalArgumentException(s"Cannot create a WitnessTxSigComponent out of $x")
  }

}

object WitnessTxSigComponentRaw {

  private case class WitnessTxSigComponentRawImpl(transaction: WitnessTransaction, inputIndex: UInt32,
    output: TransactionOutput, flags: Seq[ScriptFlag]) extends WitnessTxSigComponentRaw

  def apply(transaction: WitnessTransaction, inputIndex: UInt32,
    output: TransactionOutput, flags: Seq[ScriptFlag]): WitnessTxSigComponentRaw = {
    output.scriptPubKey match {
      case _: WitnessScriptPubKey => WitnessTxSigComponentRawImpl(transaction, inputIndex, output, flags)
      case x @ (_: P2PKScriptPubKey | _: P2PKHScriptPubKey | _: MultiSignatureScriptPubKey
        | _: P2SHScriptPubKey | _: LockTimeScriptPubKey | _: NonStandardScriptPubKey
        | _: WitnessCommitment | _: EscrowTimeoutScriptPubKey | EmptyScriptPubKey) =>
        throw new IllegalArgumentException(s"Cannot create a WitnessTxSigComponentRaw with a spk of $x")
    }

  }
}

object WitnessTxSigComponentP2SH {

  private case class WitnessTxSigComponentP2SHImpl(transaction: WitnessTransaction, inputIndex: UInt32,
    output: TransactionOutput, flags: Seq[ScriptFlag]) extends WitnessTxSigComponentP2SH

  def apply(transaction: WitnessTransaction, inputIndex: UInt32, output: TransactionOutput, flags: Seq[ScriptFlag]): WitnessTxSigComponentP2SH = {
    output.scriptPubKey match {
      case _: P2SHScriptPubKey => WitnessTxSigComponentP2SHImpl(transaction, inputIndex, output, flags)
      case x @ (_: P2PKScriptPubKey | _: P2PKHScriptPubKey | _: MultiSignatureScriptPubKey | _: LockTimeScriptPubKey
        | _: NonStandardScriptPubKey | _: EscrowTimeoutScriptPubKey
        | _: WitnessCommitment | _: WitnessScriptPubKey | EmptyScriptPubKey) =>
        throw new IllegalArgumentException(s"Cannot create a WitnessTxSigComponentP2SH with a spk of $x")
    }

  }
}

object WitnessTxSigComponentRebuilt {

  private case class WitnessTxSigComponentRebuiltImpl(transaction: WitnessTransaction, inputIndex: UInt32,
    output: TransactionOutput, witnessScriptPubKey: WitnessScriptPubKey,
    flags: Seq[ScriptFlag]) extends WitnessTxSigComponentRebuilt

  def apply(wtx: WitnessTransaction, inputIndex: UInt32, output: TransactionOutput, witScriptPubKey: WitnessScriptPubKey,
    flags: Seq[ScriptFlag]): WitnessTxSigComponentRebuilt = {
    WitnessTxSigComponentRebuiltImpl(wtx, inputIndex, output, witScriptPubKey, flags)
  }
}