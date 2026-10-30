use nexus_protocol_kde::crypto::{sha256_digest, CertificateFingerprint};
use nexus_protocol_kde::fsm::{DeviceConnectionState, PairingEvent, PairingStateMachine};
use nexus_protocol_kde::packet::{
    DeviceType, IdentityBody, KdePacket, KdePacketBody, MAX_KDE_PACKET_SIZE,
};
use nexus_protocol_kde::ProtocolError;

#[test]
fn test_sha256_known_vectors() {
    // Vector 1: Empty string
    let h1 = sha256_digest(b"");
    let hex1: String = h1.iter().map(|b| format!("{:02x}", b)).collect();
    assert_eq!(
        hex1,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );

    // Vector 2: "abc"
    let h2 = sha256_digest(b"abc");
    let hex2: String = h2.iter().map(|b| format!("{:02x}", b)).collect();
    assert_eq!(
        hex2,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn test_constant_time_fingerprint_verification() {
    let der1 = b"dummy_certificate_der_payload_123456789";
    let der2 = b"dummy_certificate_der_payload_123456780"; // 1 byte difference

    let fp1 = CertificateFingerprint::from_der(der1);
    let fp1_dup = CertificateFingerprint::from_der(der1);
    let fp2 = CertificateFingerprint::from_der(der2);

    assert!(fp1.verify_constant_time(&fp1_dup));
    assert!(!fp1.verify_constant_time(&fp2));

    let colon_str = fp1.to_colon_hex();
    assert_eq!(colon_str.len(), 32 * 3 - 1);
    assert_eq!(colon_str.chars().filter(|&c| c == ':').count(), 31);
}

#[test]
fn test_identity_packet_roundtrip() {
    let packet = KdePacket::new_identity(
        "nexus-desktop-001",
        "Nexus Workstation",
        DeviceType::Desktop,
        Some(1716),
        vec!["kdeconnect.ping".into(), "kdeconnect.pair".into()],
        vec!["kdeconnect.ping".into()],
    )
    .expect("Identity packet creation must succeed");

    let wire = packet
        .to_wire_bytes()
        .expect("Wire serialization must succeed");
    assert!(wire.ends_with(b"\n"));
    assert!(wire.len() <= MAX_KDE_PACKET_SIZE);

    let parsed = KdePacket::parse_bounded(&wire).expect("Bounded parsing must succeed");
    assert_eq!(parsed.packet_type, "kdeconnect.identity");

    match parsed.extract_body().expect("Extraction must succeed") {
        KdePacketBody::Identity(body) => {
            assert_eq!(body.device_id, "nexus-desktop-001");
            assert_eq!(body.device_name, "Nexus Workstation");
            assert_eq!(body.device_type, DeviceType::Desktop);
            assert_eq!(body.tcp_port, Some(1716));
            assert_eq!(body.incoming_capabilities.len(), 2);
        }
        other => panic!("Expected Identity body, got {:?}", other),
    }
}

#[test]
fn test_packet_too_large_rejection() {
    let huge_data = vec![b'a'; MAX_KDE_PACKET_SIZE + 10];
    let err = KdePacket::parse_bounded(&huge_data);
    match err {
        Err(ProtocolError::PacketTooLarge { actual, max }) => {
            assert_eq!(actual, MAX_KDE_PACKET_SIZE + 10);
            assert_eq!(max, MAX_KDE_PACKET_SIZE);
        }
        other => panic!("Expected PacketTooLarge error, got {:?}", other),
    }
}

#[test]
fn test_invalid_device_id_rejection() {
    // Leading dash switch injection
    let err = KdePacket::new_identity(
        "-switch-injection",
        "Hacker Device",
        DeviceType::Phone,
        None,
        vec![],
        vec![],
    );
    assert!(matches!(err, Err(ProtocolError::InvalidDeviceId(_))));

    // Space in device ID
    let err2 = KdePacket::new_identity(
        "invalid id with spaces",
        "Hacker Device",
        DeviceType::Phone,
        None,
        vec![],
        vec![],
    );
    assert!(matches!(err2, Err(ProtocolError::InvalidDeviceId(_))));
}

#[test]
fn test_pairing_fsm_lifecycle() {
    let identity = IdentityBody {
        device_id: "phone-123".into(),
        device_name: "Pixel 9 Pro".into(),
        device_type: DeviceType::Phone,
        protocol_version: 7,
        tcp_port: Some(1716),
        incoming_capabilities: vec![],
        outgoing_capabilities: vec![],
    };

    let mut fsm = PairingStateMachine::new(identity);
    assert!(!fsm.is_paired());
    assert!(!fsm.is_connected());

    // 1. Send pair request
    fsm.transition(PairingEvent::SendPairRequest)
        .expect("Transition must succeed");
    assert!(matches!(
        fsm.state(),
        DeviceConnectionState::PairingRequestedByUs { .. }
    ));

    // 2. Peer accepts pairing with cert fingerprint
    let fp = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99".to_string();
    fsm.transition(PairingEvent::AcceptPairing {
        fingerprint: fp.clone(),
    })
    .expect("Accept must succeed");
    assert!(fsm.is_paired());
    assert!(!fsm.is_connected());
    assert!(matches!(fsm.state(), DeviceConnectionState::Paired { .. }));

    // 3. TLS handshake completes
    fsm.transition(PairingEvent::TlsHandshakeSuccess)
        .expect("TLS handshake must succeed");
    assert!(fsm.is_connected());
    assert!(matches!(
        fsm.state(),
        DeviceConnectionState::Connected { .. }
    ));

    // 4. Connection dropped
    fsm.transition(PairingEvent::TlsDisconnected)
        .expect("Disconnect must succeed");
    assert!(fsm.is_paired());
    assert!(!fsm.is_connected());
    assert!(matches!(fsm.state(), DeviceConnectionState::Paired { .. }));

    // 5. Unpair
    fsm.transition(PairingEvent::UnpairCommand)
        .expect("Unpair must succeed");
    assert!(!fsm.is_paired());
    assert!(matches!(
        fsm.state(),
        DeviceConnectionState::Unpaired { .. }
    ));
}

#[test]
fn test_invalid_state_transition_fails_gracefully() {
    let identity = IdentityBody {
        device_id: "phone-456".into(),
        device_name: "Galaxy S24".into(),
        device_type: DeviceType::Phone,
        protocol_version: 7,
        tcp_port: Some(1716),
        incoming_capabilities: vec![],
        outgoing_capabilities: vec![],
    };

    let mut fsm = PairingStateMachine::new(identity);

    // Cannot jump directly from Discovered to TlsHandshakeSuccess without pairing
    let err = fsm.transition(PairingEvent::TlsHandshakeSuccess);
    assert!(matches!(
        err,
        Err(ProtocolError::InvalidStateTransition { .. })
    ));
}
