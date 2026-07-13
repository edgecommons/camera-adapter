#!/bin/sh
# Generates the ONVIF simulator's throwaway TLS material.
#
# These certificates used to be COMMITTED, private key and all. That is a bad habit even when the key
# is worthless -- and once this repository went public, GitHub's secret scanning was entirely right to
# flag it. A private key in a public repository is a private key in a public repository; the reader
# has to take our word for it that this one guards nothing.
#
# So it is minted here instead, on demand, and never tracked. The material is deliberately
# uninteresting: a self-signed test CA and one server certificate for `camera.test`, valid for ten
# years, guarding a simulator that serves fake cameras to a test suite.
#
# Idempotent: regenerates only when the material is missing or within 30 days of expiry, so a normal
# `verify.ps1` run costs nothing. Pass --force to mint a fresh set regardless.
#
# Requires openssl (present on Linux, in Git Bash on Windows, and in the validation containers).
set -eu

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/onvif_sim/fixtures/tls"
force="${1:-}"

ca_cert="$dir/ca-cert.pem"
ca_key="$dir/ca-key.pem"
server_cert="$dir/server-cert.pem"
server_key="$dir/server-key.pem"

if [ "$force" != "--force" ] && [ -f "$ca_cert" ] && [ -f "$server_cert" ] && [ -f "$server_key" ]; then
    # `-checkend` exits non-zero when the certificate expires within the given window.
    if openssl x509 -in "$server_cert" -noout -checkend 2592000 >/dev/null 2>&1; then
        echo "TLS fixtures are present and valid: $dir"
        exit 0
    fi
    echo "TLS fixtures expire within 30 days; regenerating."
fi

mkdir -p "$dir"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# The subject is declared in a config file rather than passed as `-subj "/O=.../CN=..."`, because
# Git Bash rewrites anything that looks like a POSIX path and would hand openssl
# `C:/Program Files/Git/O=EdgeCommons Test Fixtures/CN=...`. A config file has no such problem, and
# behaves identically on Linux.
cat > "$tmp/ca.cnf" <<'CNF'
[req]
distinguished_name = dn
prompt = no
x509_extensions = v3_ca

[dn]
O = EdgeCommons Test Fixtures
CN = Camera Simulator Test CA

[v3_ca]
basicConstraints = critical, CA:TRUE, pathlen:0
keyUsage = critical, digitalSignature, keyCertSign, cRLSign
CNF

cat > "$tmp/server.cnf" <<'CNF'
[req]
distinguished_name = dn
prompt = no

[dn]
O = EdgeCommons Test Fixtures
CN = camera.test
CNF

# `subjectAltName` is the load-bearing extension: the adapter pins the hostname it dialled, so a
# certificate without `camera.test` here fails hostname verification and the TLS tests fail in a way
# that looks like a bug in the adapter rather than a bad fixture.
cat > "$tmp/server.ext" <<'EXT'
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyAgreement
extendedKeyUsage = serverAuth
subjectAltName = DNS:camera.test, DNS:localhost, IP:127.0.0.1, IP:0:0:0:0:0:0:0:1
EXT

# The CA. Its key stays in the fixtures directory, which is gitignored in its entirety -- it signs
# nothing but this one simulator certificate and is regenerated whenever anyone asks.
openssl ecparam -name prime256v1 -genkey -noout -out "$ca_key"
openssl req -new -x509 -key "$ca_key" -out "$ca_cert" -days 3650 -sha256 -config "$tmp/ca.cnf"

# The server certificate for the simulator.
openssl ecparam -name prime256v1 -genkey -noout -out "$server_key"
openssl req -new -key "$server_key" -out "$tmp/server.csr" -sha256 -config "$tmp/server.cnf"
openssl x509 -req -in "$tmp/server.csr" -CA "$ca_cert" -CAkey "$ca_key" \
    -CAcreateserial -out "$server_cert" -days 3650 -sha256 -extfile "$tmp/server.ext"

chmod 600 "$ca_key" "$server_key"
chmod 644 "$ca_cert" "$server_cert"

echo "Minted throwaway TLS fixtures in $dir"
openssl x509 -in "$server_cert" -noout -subject -issuer -ext subjectAltName
