use std::str::FromStr;
use bitcoin::{Address, Network, PrivateKey, CompressedPublicKey};
use bitcoin::secp256k1::{Secp256k1, SecretKey};

#[test]
fn test_parse_bech32_address() {
    // Generate a valid regtest bech32 address and ensure parsing works
    let secp = Secp256k1::new();
    let sk = SecretKey::from_slice(&[1u8; 32]).unwrap();
    let privkey = PrivateKey::new(sk, Network::Regtest);
    let pk = privkey.public_key(&secp);
    let cpk = CompressedPublicKey::try_from(pk).unwrap();
    let addr = Address::p2wpkh(&cpk, Network::Regtest);
    let addr_str = addr.to_string();

    let address = Address::from_str(&addr_str);
    assert!(address.is_ok(), "Failed to parse bech32 address: {:?}", address.err());
}