package org.scalacoin.protocol

import org.scalatest.{FlatSpec, MustMatchers}

class BitcoinAddressTest extends FlatSpec with MustMatchers {

  "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy" must "be a valid bitcoin address" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"
    BitcoinAddress(address).bitcoinAddress must be(address)

  }

  "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy" must "be a valid p2sh address and not a valid p2pkh" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"
    BitcoinAddress.p2shAddress(address) must be (true)
    BitcoinAddress.p2pkh(address) must be (false)
  }

  "17WN1kFw8D6w1eHzqvkh49xwjE3iPN925b" must "be a valid p2pkh" in {
    val address = "17WN1kFw8D6w1eHzqvkh49xwjE3iPN925b"
    BitcoinAddress.p2pkh(address) must be (true)
    BitcoinAddress.p2shAddress(address) must be (false)
  }

  "The empty string" must "not be a valid bitcoin address" in {
    intercept[IllegalArgumentException] {
      BitcoinAddress("")
    }
  }
  "A string that is 25 characters long" must "not be a valid bitcoin address" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiW"
    intercept[IllegalArgumentException] {
      BitcoinAddress(address)
    }
  }

  "A string that is 36 characters long" must "not be a valid bitcoin address" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLyyy"
    intercept[IllegalArgumentException] {
      BitcoinAddress(address)
    }
  }

  "3J98t1WpEZ73CNmQviecrnyiWr (26 characters) " must "be a valid bitcoin address" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiWr"
    BitcoinAddress(address).bitcoinAddress must be(address)
  }

  "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLyy (35 characters)" must "be a valid bitcoin address" in {
    val address = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLyy"
    BitcoinAddress(address).bitcoinAddress must be(address)
  }




  "akJsoCcyh34FGPotxfEoSXGwFPCNAkyCgTA" must "be a valid asset address" in {
    val assetAddress = AssetAddress("akJsoCcyh34FGPotxfEoSXGwFPCNAkyCgTA")
    assetAddress.assetAddress must be ("akJsoCcyh34FGPotxfEoSXGwFPCNAkyCgTA")
  }

  "An asset address with the first character replaced" must "not be a valid asset address" in {
    //3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLyy
    intercept[org.bitcoinj.core.AddressFormatException] {
      val assetAddress = AssetAddress("aJ98t1WpEZ73CNmQviecrnyiWrnqRhWNLyy")
    }
  }
}