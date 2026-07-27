use super::*;
use clap::{CommandFactory, FromArgMatches};
use std::process::Command;

const ROLLUP_ENV_CHILD: &str = "EEZ_PROOF_SIGNER_ROLLUP_ENV_TEST_CHILD";
const ROLLUP_ENV_CHILD_OK: &str = "rollup-env-child-ok";
const TEST_VKEY: &str = "4242424242424242424242424242424242424242424242424242424242424242";
// Anvil account #1 is the independent test attester.
const TEST_ATTESTER_KEY: &str = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const PREFIXED_TEST_ATTESTER_KEY: &str =
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const TEST_ATTESTER_ADDRESS: &str = "70997970c51812dc3A010C7d01b50e0d17dc79C8";
// Anvil account #0 derives the protocol's reserved L2 system address.
const TEST_SYSTEM_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const PREFIXED_TEST_SYSTEM_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const TEST_PROOF_SYSTEM: &str = "00000000000000000000000000000000000000aa";

/// Parse a normal valid baseline while keeping each test focused on the option
/// it varies. Tests for mandatory options use `parse_exact_args` directly.
fn parse_args<const N: usize>(args: [&str; N]) -> Result<Args, clap::Error> {
    let mut args = args.to_vec();
    if !args.contains(&"--vkey") {
        args.extend(["--vkey", TEST_VKEY]);
    }
    if !args.contains(&"--signer-key") {
        args.extend(["--signer-key", TEST_ATTESTER_KEY]);
    }
    if !args.contains(&"--l2-system-key") {
        args.extend(["--l2-system-key", TEST_SYSTEM_KEY]);
    }
    if !args.contains(&"--proof-system") {
        args.extend(["--proof-system", TEST_PROOF_SYSTEM]);
    }
    parse_exact_args(args)
}

/// Parse only the supplied command line, ignoring ambient `EEZ_*` variables.
fn parse_exact_args<I, T>(args: I) -> Result<Args, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let matches = Args::command()
        .mut_args(|arg| arg.env(None::<&'static str>))
        .try_get_matches_from(args)?;
    Args::from_arg_matches(&matches)
}

#[test]
fn cli_parses_the_default_listen_addr() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "7",
    ])
    .unwrap();
    let config = Config::from_args(args).unwrap();
    assert_eq!(config.listen_addr, "127.0.0.1:50061".parse().unwrap());
    assert_eq!(
        config.chain_document_path,
        PathBuf::from("chain-config.json")
    );
    assert_eq!(config.expected_rollup_id.get(), 7);
    assert_eq!(
        config.attester.proof_system_vkey().get(),
        B256::repeat_byte(0x42)
    );
    assert_eq!(
        config.attester.address(),
        TEST_ATTESTER_ADDRESS.parse::<Address>().unwrap()
    );
    assert_eq!(
        config.attester.expected_proof_system(),
        TEST_PROOF_SYSTEM.parse::<Address>().unwrap()
    );
    assert_eq!(
        config.limits,
        ServiceLimits::new(ServiceLimitsParams {
            max_window_blocks: NonZeroUsize::new(512).unwrap(),
            max_window_bytes: NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
            max_window_witness_items: NonZeroUsize::new(1_000_000).unwrap(),
            max_transaction_state_checkpoints: 8,
            stream_idle_timeout: Duration::from_mins(2),
            request_timeout: Duration::from_mins(10),
        })
        .unwrap()
    );
}

#[test]
fn listen_addr_flag_overrides_the_default() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--listen-addr",
        "127.0.0.1:9",
    ])
    .unwrap();
    assert_eq!(
        Config::from_args(args).unwrap().listen_addr,
        "127.0.0.1:9".parse().unwrap()
    );
}

#[test]
fn a_malformed_listen_addr_is_rejected() {
    assert!(
        parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--listen-addr",
            "not-an-addr",
        ])
        .is_err()
    );
}

#[test]
fn chain_config_is_mandatory() {
    assert!(parse_args(["eez-proof-signer", "--rollup-id", "1"]).is_err());
}

#[test]
fn rollup_id_is_mandatory_and_nonzero() {
    assert!(parse_args(["eez-proof-signer", "--chain-config", "chain-config.json"]).is_err());
    assert!(
        parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn malformed_rollup_ids_are_rejected() {
    for value in ["", "not-a-number", "0x1", "-1", "18446744073709551616"] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                value,
            ])
            .is_err(),
            "accepted invalid rollup ID {value:?}"
        );
    }
}

/// Exercise clap's real environment handling in an isolated child process;
/// mutating this process's environment would race the other unit tests.
#[test]
fn rollup_id_uses_environment_fallback_with_cli_precedence() {
    if let Ok(scenario) = std::env::var(ROLLUP_ENV_CHILD) {
        let cli = match scenario.as_str() {
            "fallback" | "empty" => vec![
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--vkey",
                TEST_VKEY,
                "--signer-key",
                TEST_ATTESTER_KEY,
                "--l2-system-key",
                TEST_SYSTEM_KEY,
                "--proof-system",
                TEST_PROOF_SYSTEM,
            ],
            "cli-precedence" => vec![
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "9",
                "--vkey",
                TEST_VKEY,
                "--signer-key",
                TEST_ATTESTER_KEY,
                "--l2-system-key",
                TEST_SYSTEM_KEY,
                "--proof-system",
                TEST_PROOF_SYSTEM,
            ],
            _ => panic!("unknown rollup environment test scenario {scenario:?}"),
        };
        let parsed = Args::command()
            .try_get_matches_from(cli)
            .and_then(|matches| Args::from_arg_matches(&matches));
        match scenario.as_str() {
            "fallback" => assert_eq!(parsed.unwrap().expected_rollup_id.get(), 7),
            "cli-precedence" => assert_eq!(parsed.unwrap().expected_rollup_id.get(), 9),
            "empty" => assert!(parsed.is_err(), "an empty EEZ_ROLLUP_ID was accepted"),
            _ => unreachable!(),
        }
        eprintln!("{ROLLUP_ENV_CHILD_OK}:{scenario}");
        return;
    }

    for (scenario, environment_value) in [("fallback", "7"), ("cli-precedence", "7"), ("empty", "")]
    {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child.args([
            "--exact",
            "config::tests::rollup_id_uses_environment_fallback_with_cli_precedence",
            "--nocapture",
        ]);

        // Preserve the normal test environment, but prevent unrelated EEZ
        // configuration from affecting the child process.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("EEZ_") {
                child.env_remove(name);
            }
        }
        child
            .env(ROLLUP_ENV_CHILD, scenario)
            .env("EEZ_ROLLUP_ID", environment_value);

        let output = child.output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "rollup environment scenario {scenario:?} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("{ROLLUP_ENV_CHILD_OK}:{scenario}"))
                || stderr.contains(&format!("{ROLLUP_ENV_CHILD_OK}:{scenario}")),
            "rollup environment child did not run for {scenario:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
}

#[test]
fn vkey_accepts_exactly_32_hex_bytes_with_an_optional_prefix() {
    const PREFIXED: &str = "0x4242424242424242424242424242424242424242424242424242424242424242";
    const BARE: &str = "4242424242424242424242424242424242424242424242424242424242424242";
    for value in [PREFIXED, BARE] {
        let args = parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--vkey",
            value,
        ])
        .unwrap();
        assert_eq!(
            Config::from_args(args)
                .unwrap()
                .attester
                .proof_system_vkey()
                .get(),
            B256::repeat_byte(0x42)
        );
    }
}

#[test]
fn vkey_is_mandatory_and_nonzero() {
    assert!(
        parse_exact_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--signer-key",
            TEST_ATTESTER_KEY,
            "--l2-system-key",
            TEST_SYSTEM_KEY,
            "--proof-system",
            TEST_PROOF_SYSTEM,
        ])
        .is_err()
    );
    for zero in [
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
    ] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "1",
                "--vkey",
                zero,
            ])
            .is_err()
        );
    }
}

#[test]
fn malformed_vkeys_are_rejected() {
    for value in [
        "",
        "0x42",
        "0xgggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
        "0x424242424242424242424242424242424242424242424242424242424242424242",
    ] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "1",
                "--vkey",
                value,
            ])
            .is_err(),
            "accepted malformed vkey {value:?}"
        );
    }
}

#[test]
fn secret_key_args_accept_exactly_32_hex_bytes_with_one_optional_prefix() {
    // Pin the accepted shapes independently of the underlying hex parser:
    // exactly 64 hex digits, any case, behind at most one 0x or 0X prefix.
    let bare = "abababababababababababababababababababababababababababababababab";
    for accepted in [
        bare.to_owned(),
        format!("0x{bare}"),
        format!("0X{bare}"),
        format!("0x{}", bare.to_uppercase()),
    ] {
        let key = accepted
            .parse::<SecretKeyArg>()
            .unwrap()
            .into_key("test")
            .unwrap_or_else(|_| panic!("rejected well-formed key {accepted:?}"));
        assert_eq!(key, B256::repeat_byte(0xab));
    }
    for rejected in [
        String::new(),
        bare[..62].to_owned(),
        format!("{bare}ab"),
        format!("0x0X{bare}"),
        format!("zz{}", &bare[2..]),
        format!(" {bare}"),
    ] {
        assert!(
            rejected
                .parse::<SecretKeyArg>()
                .unwrap()
                .into_key("test")
                .is_err(),
            "accepted malformed key {rejected:?}"
        );
    }
}

#[test]
fn signer_key_accepts_exactly_32_hex_bytes_with_an_optional_prefix() {
    for value in [TEST_ATTESTER_KEY, PREFIXED_TEST_ATTESTER_KEY] {
        let args = parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--signer-key",
            value,
        ])
        .unwrap();

        let config = Config::from_args(args).unwrap();
        assert_eq!(
            config.attester.address(),
            TEST_ATTESTER_ADDRESS.parse::<Address>().unwrap()
        );
        // The operator-supplied proof-system vkey is independent of the signer address.
        assert_eq!(
            config.attester.proof_system_vkey().get(),
            B256::repeat_byte(0x42)
        );
    }
}

#[test]
fn signer_key_is_mandatory() {
    assert!(
        parse_exact_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--vkey",
            TEST_VKEY,
            "--l2-system-key",
            TEST_SYSTEM_KEY,
            "--proof-system",
            TEST_PROOF_SYSTEM,
        ])
        .is_err()
    );
}

#[test]
fn malformed_and_invalid_attestation_keys_are_rejected_without_echoing_them() {
    const CURVE_ORDER: &str = "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141";
    const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const NON_HEX: &str = "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg";

    for (value, expected) in [
        (
            "",
            "attestation key must contain exactly 32 bytes of hexadecimal",
        ),
        (
            "42",
            "attestation key must contain exactly 32 bytes of hexadecimal",
        ),
        (
            NON_HEX,
            "attestation key must contain exactly 32 bytes of hexadecimal",
        ),
        (
            "424242424242424242424242424242424242424242424242424242424242424242",
            "attestation key must contain exactly 32 bytes of hexadecimal",
        ),
        (ZERO, "attestation key is not a valid secp256k1 private key"),
        (
            CURVE_ORDER,
            "attestation key is not a valid secp256k1 private key",
        ),
    ] {
        let args = parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--signer-key",
            value,
        ])
        .unwrap();
        let error = Config::from_args(args).unwrap_err().to_string();
        assert_eq!(error, expected);
        assert!(value.is_empty() || !error.contains(value));
    }
}

#[test]
fn signer_key_must_not_be_the_l2_system_key() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--signer-key",
        TEST_SYSTEM_KEY,
    ])
    .unwrap();

    let error = Config::from_args(args).unwrap_err().to_string();

    assert_eq!(
        error,
        "attestation key must not derive the reserved L2 system address"
    );
    assert!(!error.contains(TEST_SYSTEM_KEY));
}

#[test]
fn signer_key_is_redacted_from_debug_help_and_clap_errors() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--signer-key",
        TEST_ATTESTER_KEY,
    ])
    .unwrap();
    let debug = format!("{args:?}");
    assert!(!debug.contains(TEST_ATTESTER_KEY));
    assert!(debug.contains("<redacted>"));
    let config = Config::from_args(args).unwrap();
    assert!(!format!("{config:?}").contains(TEST_ATTESTER_KEY));

    let command = Args::command();
    let signer_arg = command
        .get_arguments()
        .find(|argument| argument.get_id() == "attestation_key")
        .unwrap();
    assert!(signer_arg.is_hide_env_values_set());

    let error = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "not-a-number",
        "--signer-key",
        TEST_ATTESTER_KEY,
    ])
    .unwrap_err()
    .to_string();
    assert!(!error.contains(TEST_ATTESTER_KEY));
}

#[test]
fn l2_system_key_is_mandatory_and_pinned_to_the_reserved_address() {
    assert!(
        parse_exact_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--vkey",
            TEST_VKEY,
            "--signer-key",
            TEST_ATTESTER_KEY,
            "--proof-system",
            TEST_PROOF_SYSTEM,
        ])
        .is_err()
    );

    const OTHER_VALID_KEY: &str =
        "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--l2-system-key",
        OTHER_VALID_KEY,
    ])
    .unwrap();
    let error = Config::from_args(args).unwrap_err().to_string();
    assert!(error.contains(&format!("expected {}", eez_evm::SYSTEM_ADDRESS)));
    assert!(!error.contains(OTHER_VALID_KEY));
}

#[test]
fn l2_system_key_is_redacted_and_rejects_invalid_secret_material() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--l2-system-key",
        PREFIXED_TEST_SYSTEM_KEY,
    ])
    .unwrap();
    assert!(!format!("{args:?}").contains(TEST_SYSTEM_KEY));
    let config = Config::from_args(args).unwrap();
    assert!(!format!("{config:?}").contains(TEST_SYSTEM_KEY));

    let command = Args::command();
    let key_arg = command
        .get_arguments()
        .find(|argument| argument.get_id() == "system_transaction_key")
        .unwrap();
    assert!(key_arg.is_hide_env_values_set());

    let zero = "0000000000000000000000000000000000000000000000000000000000000000";
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--l2-system-key",
        zero,
    ])
    .unwrap();
    let error = Config::from_args(args).unwrap_err().to_string();
    assert_eq!(error, "L2 system key is not a valid secp256k1 private key");
    assert!(!error.contains(zero));
}

#[test]
fn proof_system_is_mandatory_and_nonzero() {
    assert!(
        parse_exact_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--vkey",
            TEST_VKEY,
            "--signer-key",
            TEST_ATTESTER_KEY,
            "--l2-system-key",
            TEST_SYSTEM_KEY,
        ])
        .is_err()
    );

    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--proof-system",
        "0000000000000000000000000000000000000000",
    ])
    .unwrap();
    assert_eq!(
        Config::from_args(args).unwrap_err().to_string(),
        "proof-system address must be non-zero"
    );
}

#[test]
fn malformed_proof_system_addresses_are_rejected() {
    for value in ["", "0xaa", "not-an-address"] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "1",
                "--proof-system",
                value,
            ])
            .is_err(),
            "accepted malformed proof-system address {value:?}"
        );
    }
}

#[test]
fn zero_resource_limits_are_rejected() {
    for flag in [
        "--max-request-blocks",
        "--max-request-bytes",
        "--max-request-witness-items",
        "--stream-idle-timeout-secs",
        "--request-timeout-secs",
    ] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "1",
                flag,
                "0",
            ])
            .is_err(),
            "{flag} accepted zero"
        );
    }
}

#[test]
fn the_removed_concurrency_option_is_rejected() {
    assert!(
        parse_args([
            "eez-proof-signer",
            "--chain-config",
            "chain-config.json",
            "--rollup-id",
            "1",
            "--max-concurrent-requests",
            "1",
        ])
        .is_err()
    );
}

#[test]
fn zero_transaction_state_checkpoint_limit_is_allowed() {
    let args = parse_args([
        "eez-proof-signer",
        "--chain-config",
        "chain-config.json",
        "--rollup-id",
        "1",
        "--max-transaction-state-checkpoints",
        "0",
    ])
    .unwrap();

    let config = Config::from_args(args).unwrap();
    assert_eq!(config.limits.max_transaction_state_checkpoints(), 0);
}

#[test]
fn malformed_transaction_state_checkpoint_limits_are_rejected() {
    for value in ["-1", "not-a-number", "18446744073709551616"] {
        assert!(
            parse_args([
                "eez-proof-signer",
                "--chain-config",
                "chain-config.json",
                "--rollup-id",
                "1",
                "--max-transaction-state-checkpoints",
                value,
            ])
            .is_err(),
            "accepted invalid checkpoint limit {value:?}",
        );
    }
}
