#!/usr/bin/env bash
# Print the local devnet endpoints. Source this file to export them as
# EEZ_DEVNET_* variables in the current shell.

eez_devnet_ports_main() {
    local enclave="${KURTOSIS_ENCLAVE:-eez-ci}"

    command -v kurtosis >/dev/null || {
        echo "kurtosis not found in PATH" >&2
        return 1
    }

    eez_devnet_port() {
        local service="$1" port="$2" value
        value="$(kurtosis port print "$enclave" "$service" "$port" 2>/dev/null)" || {
            echo "could not resolve $service/$port in enclave '$enclave'" >&2
            return 1
        }
        [[ -n "$value" ]] || {
            echo "empty endpoint for $service/$port in enclave '$enclave'" >&2
            return 1
        }
        printf '%s\n' "$value"
    }

    eez_devnet_http_url() {
        case "$1" in
            http://*|https://*) printf '%s\n' "$1" ;;
            *) printf 'http://%s\n' "$1" ;;
        esac
    }

    local l1_rpc l2_rpc l1_front l2_front proof_signer_grpc
    local l1_explorer l2_explorer l1_explorer_api l2_explorer_api
    l1_rpc="$(eez_devnet_port el-1-reth-lighthouse rpc)" || return 1
    l2_rpc="$(eez_devnet_port eez-node l2-rpc)" || return 1
    l1_front="$(eez_devnet_port eez-node l1-xchain)" || return 1
    l2_front="$(eez_devnet_port eez-node l2-xchain)" || return 1
    proof_signer_grpc="$(eez_devnet_port eez-proof-signer grpc)" || return 1
    l1_explorer="$(eez_devnet_port l1-blockscout-frontend http 2>/dev/null || true)"
    l2_explorer="$(eez_devnet_port l2-blockscout-frontend http 2>/dev/null || true)"
    l1_explorer_api="$(eez_devnet_port l1-blockscout http 2>/dev/null || true)"
    l2_explorer_api="$(eez_devnet_port l2-blockscout http 2>/dev/null || true)"

    export EEZ_DEVNET_L1_RPC="$(eez_devnet_http_url "$l1_rpc")"
    export EEZ_DEVNET_L2_RPC="$(eez_devnet_http_url "$l2_rpc")"
    export EEZ_DEVNET_L1_FRONT="$(eez_devnet_http_url "$l1_front")"
    export EEZ_DEVNET_L2_FRONT="$(eez_devnet_http_url "$l2_front")"
    export EEZ_DEVNET_PROOF_SIGNER_GRPC="$proof_signer_grpc"
    unset EEZ_DEVNET_L1_EXPLORER EEZ_DEVNET_L2_EXPLORER
    unset EEZ_DEVNET_L1_EXPLORER_API EEZ_DEVNET_L2_EXPLORER_API
    if [[ -n "$l1_explorer" ]]; then
        export EEZ_DEVNET_L1_EXPLORER="$(eez_devnet_http_url "$l1_explorer")"
    fi
    if [[ -n "$l2_explorer" ]]; then
        export EEZ_DEVNET_L2_EXPLORER="$(eez_devnet_http_url "$l2_explorer")"
    fi
    if [[ -n "$l1_explorer_api" ]]; then
        export EEZ_DEVNET_L1_EXPLORER_API="$(eez_devnet_http_url "$l1_explorer_api")"
    fi
    if [[ -n "$l2_explorer_api" ]]; then
        export EEZ_DEVNET_L2_EXPLORER_API="$(eez_devnet_http_url "$l2_explorer_api")"
    fi

    printf '\nEEZ Kurtosis devnet endpoints (enclave: %s)\n' "$enclave"
    printf '  %-22s %s\n' 'Canonical L1 RPC:' "$EEZ_DEVNET_L1_RPC"
    printf '  %-22s %s\n' 'L2 RPC:' "$EEZ_DEVNET_L2_RPC"
    printf '  %-22s %s\n' 'L1 cross-chain front:' "$EEZ_DEVNET_L1_FRONT"
    printf '  %-22s %s\n' 'L2 cross-chain front:' "$EEZ_DEVNET_L2_FRONT"
    printf '  %-22s %s\n' 'Proof signer gRPC:' "$EEZ_DEVNET_PROOF_SIGNER_GRPC"
    if [[ -n "${EEZ_DEVNET_L1_EXPLORER:-}" ]]; then
        printf '  %-22s %s\n' 'L1 explorer:' "$EEZ_DEVNET_L1_EXPLORER"
    fi
    if [[ -n "${EEZ_DEVNET_L2_EXPLORER:-}" ]]; then
        printf '  %-22s %s\n' 'L2 explorer:' "$EEZ_DEVNET_L2_EXPLORER"
    fi
    if [[ -n "${EEZ_DEVNET_L1_EXPLORER_API:-}" ]]; then
        printf '  %-22s %s\n' 'L1 explorer API:' "$EEZ_DEVNET_L1_EXPLORER_API"
    fi
    if [[ -n "${EEZ_DEVNET_L2_EXPLORER_API:-}" ]]; then
        printf '  %-22s %s\n' 'L2 explorer API:' "$EEZ_DEVNET_L2_EXPLORER_API"
    fi
}

if eez_devnet_ports_main; then
    eez_devnet_ports_status=0
else
    eez_devnet_ports_status=$?
fi
unset -f eez_devnet_ports_main eez_devnet_port eez_devnet_http_url

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    if (( eez_devnet_ports_status != 0 )); then
        unset eez_devnet_ports_status
        return 1
    fi
    unset eez_devnet_ports_status
    return 0
fi
exit "$eez_devnet_ports_status"
