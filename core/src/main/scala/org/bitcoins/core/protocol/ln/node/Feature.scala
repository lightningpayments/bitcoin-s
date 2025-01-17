package org.bitcoins.core.protocol.ln.node

sealed trait FeatureSupport

object FeatureSupport {

  case object Mandatory extends FeatureSupport {
    override def toString: String = "mandatory"
  }

  case object Optional extends FeatureSupport {
    override def toString: String = "optional"
  }
}

sealed trait Feature {
  def rfcName: String
  def mandatory: Int
  def optional: Int = mandatory + 1

  def supportBit(support: FeatureSupport): Int =
    support match {
      case FeatureSupport.Mandatory => mandatory
      case FeatureSupport.Optional  => optional
    }

  override def toString = rfcName
}

object Feature {

  case object OptionDataLossProtect extends Feature {
    val rfcName = "option_data_loss_protect"
    val mandatory = 0
  }

  case object InitialRoutingSync extends Feature {
    val rfcName = "initial_routing_sync"
    // reserved but not used as per lightningnetwork/lightning-rfc/pull/178
    val mandatory = 2
  }

  case object OptionUpfrontShutdownScript extends Feature {
    val rfcName = "option_upfront_shutdown_script"
    val mandatory = 4
  }

  case object ChannelRangeQueries extends Feature {
    val rfcName = "gossip_queries"
    val mandatory = 6
  }

  case object VariableLengthOnion extends Feature {
    val rfcName = "var_onion_optin"
    val mandatory = 8
  }

  case object ChannelRangeQueriesExtended extends Feature {
    val rfcName = "gossip_queries_ex"
    val mandatory = 10
  }

  case object StaticRemoteKey extends Feature {
    val rfcName = "option_static_remotekey"
    val mandatory = 12
  }

  case object PaymentSecret extends Feature {
    val rfcName = "payment_secret"
    val mandatory = 14
  }

  case object BasicMultiPartPayment extends Feature {
    val rfcName = "basic_mpp"
    val mandatory = 16
  }

  case object Wumbo extends Feature {
    val rfcName = "option_support_large_channel"
    val mandatory = 18
  }

  case object AnchorOutputs extends Feature {
    val rfcName = "option_anchor_outputs"
    val mandatory = 20
  }

  case object AnchorOutputsZeroFeeHtlcTx extends Feature {
    val rfcName = "option_anchors_zero_fee_htlc_tx"
    val mandatory = 22
  }

  case object ShutdownAnySegwit extends Feature {
    val rfcName = "option_shutdown_anysegwit"
    val mandatory = 26
  }

  // TODO: @t-bast: update feature bits once spec-ed (currently reserved here: https://github.com/lightningnetwork/lightning-rfc/issues/605)
  // We're not advertising these bits yet in our announcements, clients have to assume support.
  // This is why we haven't added them yet to `areSupported`.
  case object TrampolinePayment extends Feature {
    val rfcName = "trampoline_payment"
    val mandatory = 50
  }

  case object KeySend extends Feature {
    val rfcName = "keysend"
    val mandatory = 54
  }

  val knownFeatures: Set[Feature] = Set(
    OptionDataLossProtect,
    InitialRoutingSync,
    OptionUpfrontShutdownScript,
    ChannelRangeQueries,
    VariableLengthOnion,
    ChannelRangeQueriesExtended,
    PaymentSecret,
    BasicMultiPartPayment,
    Wumbo,
    TrampolinePayment,
    StaticRemoteKey,
    AnchorOutputs,
    AnchorOutputsZeroFeeHtlcTx,
    ShutdownAnySegwit,
    KeySend
  )
}
