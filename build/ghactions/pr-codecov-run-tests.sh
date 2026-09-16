#!/usr/bin/env bash

# Runs test coverage for the given space-separated list of packages (e.g.
# "pkg/util/log/logpb pkg/util/quotapool") using the native Go toolchain and
# writes a Codecov-compatible JSON summary to the given output file.
#
# This used to be driven by Bazel; it now shells out to `go test
# -coverprofile` and converts the Go coverage profile to lcov, which is then
# fed to the same lcov2json converter the code-cover-publish workflow expects.

set -xeuo pipefail

output_json_file="$1"
packages="$2"

if [ -z "${packages}" ]; then
  echo "No packages; skipping"
  touch "${output_json_file}"
  exit 0
fi

# The build is pure Go (CGO_ENABLED=0); no C dependencies need to be built.
export CGO_ENABLED=0

# Convert package directories (pkg/foo/bar) to Go package patterns (./pkg/foo/bar).
pkgs=()
for p in ${packages}; do
  if [ -d "${p}" ]; then
    pkgs+=("./${p}")
  fi
done

if [ ${#pkgs[@]} -eq 0 ]; then
  echo "No test targets found"
  exit 0
fi

echo "Running tests"

go test -tags crdb_test -covermode=atomic -coverprofile=cover.out "${pkgs[@]}"

# Convert the Go coverage profile to lcov. Module-relative paths are stripped
# so that the resulting lcov/JSON matches the "pkg/..." paths that the
# downstream tooling expects.
lcov_file=lcov.info
: > "${lcov_file}"
awk '
  NR > 1 && NF == 3 {
    split($1, b, ":")
    file = b[1]
    split(b[2], c, ",")
    split(c[1], d, ".")
    sub(/^github.com\/cockroachdb\/cockroach\//, "", file)
    if (file != prev) {
      if (prev != "") printf "LF:%d\nLH:%d\nend_of_record\n", lf[prev], lh[prev]
      printf "TN:\nSF:%s\n", file
      prev = file
      lf[prev] = 0; lh[prev] = 0
    }
    printf "DA:%s,%s\n", d[1], $3
    lf[prev]++
    if ($3 > 0) lh[prev]++
  }
  END { if (prev != "") printf "LF:%d\nLH:%d\nend_of_record\n", lf[prev], lh[prev] }
' cover.out >> "${lcov_file}"

# Convert lcov to the JSON format consumed by the code-cover-publish job.
go run github.com/cockroachdb/code-cov-utils/lcov2json@v1.0.0 "${lcov_file}" "${output_json_file}"
