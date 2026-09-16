# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# Lightweight native build for CockroachDB, driven entirely by the standard
# Go toolchain (`go build` / `go test`). There is no Bazel: generated Go code
# (*.pb.go, parsers, etc.) is checked into the tree, and the C/C++
# dependencies (jemalloc, PROJ, GEOS, optionally Kerberos 5) are built with
# their own autotools/CMake build systems into ./.build/.
#
# Run `make help` for the list of targets.

# This tree was written for the Go 1.19/1.20 series; parts of the code
# (e.g. pkg/util/goschedstats) mirror runtime internals and are intentionally
# capped at go1.20, so newer toolchains do not compile them. Install e.g.
# go1.19.10 (https://dl.google.com/go/go1.19.10.linux-amd64.tar.gz) and invoke
# `make GO=/path/to/go1.19.10/bin/go ...`, or set GO in a local Makefile.local
# file (gitignored) so you don't have to pass it on every command line.
-include $(CURDIR)/Makefile.local

GO        ?= go
# gofmt ships with the Go distribution; point GOFMT elsewhere if needed.
GOFMT     ?= gofmt

# Many recipes below (and several awk/sed pipelines) rely on bash; also force
# PWD so that tools see a physical working directory without symlinks (the Go
# toolchain does not handle symlinks well).
export SHELL := env PWD=$(CURDIR) /bin/bash
ifeq ($(SHELL),)
$(error bash is required)
endif

# This tree was written for the Go 1.19/1.20 series; parts of the code
# (e.g. pkg/util/goschedstats) mirror runtime internals and are intentionally
# capped at go1.20, so newer toolchains do not compile them. Install e.g.
# go1.19.10 (https://dl.google.com/go/go1.19.10.linux-amd64.tar.gz) and run
# `make GO=/path/to/go1.19.10/bin/go ...`.
.PHONY: check-go
check-go:
	@series=$$($(GO) version 2>/dev/null | grep -oE 'go1\.[0-9]+' | head -1); \
	case $$series in \
	  go1.19|go1.20) : ;; \
	  "") echo "error: no Go toolchain found (\$$GO)" >&2; exit 1 ;; \
	  *) echo "error: \`$$series\` is too new for this tree; CockroachDB 23.1 builds with go1.19/go1.20." >&2; \
	     echo "       Install go1.19.10 and invoke e.g. \`make GO=~/.local/go1.19/bin/go build" >&2; exit 1 ;; \
	esac

BUILD_DIR := $(CURDIR)/.build
BIN_DIR   := $(CURDIR)/bin

# Host triple, e.g. x86_64-linux-gnu. Used only for linker metadata.
HOST_TRIPLE := $(shell $(CC) -dumpmachine 2>/dev/null)
ifeq ($(strip $(HOST_TRIPLE)),)
HOST_TRIPLE := $(shell cc -dumpmachine 2>/dev/null || gcc -dumpmachine 2>/dev/null)
endif

UNAME   := $(shell uname)
NCPUS   := $(shell { getconf _NPROCESSORS_ONLN || sysctl -n hw.ncpu || nproc; } 2>/dev/null)
GIT_TAG := $(shell git describe --tags --dirty 2>/dev/null || echo local-build)
GIT_REV := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

GOEXE          := $(shell $(GO) env GOEXE 2>/dev/null)
COCKROACH      := ./cockroach$(GOEXE)
COCKROACHOSS   := ./cockroachoss$(GOEXE)
COCKROACHSHORT := ./cockroachshort$(GOEXE)

# Which package to run tests/vet against, e.g. `make test PKG=./pkg/sql`.
PKG         ?= ./pkg/...
# Regex of tests to run with `make test` (passed to `go test -run`).
TESTS       ?= .
# Test timeout.
TESTTIMEOUT ?= 60m
# Extra flags passed to `go build`/`go test` (e.g. -v).
TESTFLAGS   ?=
# Space-separated list of extra Go build tags.
TAGS        ?=
# Flags passed verbatim to the go build tool.
GOFLAGS     ?=

# STDMALLOC=1 skips jemalloc and uses the system allocator.
# GSS=1 builds and links Kerberos 5 support (Linux only; adds the `gss` tag).
# SKIP_GEOS=1 does not build the GEOS shared library (spatial builtins then
# unavailable at run time).
STDMALLOC ?=
GSS       ?=
SKIP_GEOS ?=

prefix  ?= /usr/local
bindir  ?= $(prefix)/bin
INSTALL ?= install

# ---------------------------------------------------------------------------
# Build tags and linker flags
# ---------------------------------------------------------------------------

BUILD_TAGS := $(TAGS)
ifdef STDMALLOC
BUILD_TAGS += stdmalloc
endif
ifneq (,$(findstring linux-gnu,$(HOST_TRIPLE)))
ifdef GSS
BUILD_TAGS += gss
endif
endif

# Tests run with crdb_test, which enables extra internal assertions
# (e.g. leaktest).
TEST_TAGS := $(BUILD_TAGS) crdb_test

override LDFLAGS := \
	-X "github.com/cockroachdb/cockroach/pkg/build.typ=development" \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(GIT_TAG)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(GIT_REV)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.cgoTargetTriple=$(HOST_TRIPLE)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')"

GO_BUILD := $(GO) build $(GOFLAGS) -tags '$(BUILD_TAGS)' -ldflags '$(LDFLAGS)'

# ---------------------------------------------------------------------------
# C/C++ dependencies (built into .build/, nothing is installed system-wide)
# ---------------------------------------------------------------------------

C_DEPS_DIR   := $(CURDIR)/c-deps
JEMALLOC_DIR := $(BUILD_DIR)/jemalloc
GEOS_DIR     := $(BUILD_DIR)/geos
PROJ_DIR     := $(BUILD_DIR)/proj
KRB5_DIR     := $(BUILD_DIR)/krb5

DYN_EXT := so
ifeq ($(UNAME),Darwin)
DYN_EXT := dylib
endif

LIBJEMALLOC := $(JEMALLOC_DIR)/lib/libjemalloc.a
LIBPROJ     := $(PROJ_DIR)/lib/libproj.a
LIBKRB5     := $(KRB5_DIR)/lib/libgssapi_krb5.a
LIBGEOS     := lib/libgeos_c.$(DYN_EXT)

# Every cgo package needs the include/library search paths of the C
# dependencies. We inject them into files named zcgo_flags.go (gitignored) so
# that plain `go build`/`go test` also work on these packages without going
# through Make, for whatever configuration was most recently built with Make.
CGO_PKGS := pkg/cli pkg/cli/clisqlshell pkg/server/status pkg/geo/geoproj
ifneq (,$(findstring linux-gnu,$(HOST_TRIPLE)))
ifdef GSS
CGO_PKGS += pkg/ccl/gssapiccl
endif
endif
ZCGO_FILES := $(addprefix ./,$(addsuffix /zcgo_flags.go,$(CGO_PKGS)))

$(ZCGO_FILES): Makefile | $(BUILD_DIR)
	@echo "regenerating $@"
	@printf '%s\n' \
	  '// GENERATED FILE DO NOT EDIT' \
	  '// Code generated by the Makefile. DO NOT EDIT.' \
	  '' \
	  "package $(notdir $(@D))" \
	  '' \
	  '// #cgo CPPFLAGS: -I$(JEMALLOC_DIR)/include $(if $(GSS),-I$(KRB5_DIR)/include)' \
	  '// #cgo LDFLAGS: $(addprefix -L,$(JEMALLOC_DIR)/lib $(PROJ_DIR)/lib $(if $(GSS),$(KRB5_DIR)/lib))' \
	  'import "C"' \
	> $@

# The libraries that must exist before the Go toolchain can link a binary.
C_DEPS_GO := $(if $(STDMALLOC),,$(LIBJEMALLOC)) $(LIBPROJ)
ifneq (,$(findstring linux-gnu,$(HOST_TRIPLE)))
ifdef GSS
C_DEPS_GO += $(LIBKRB5)
endif
endif

.PHONY: c-deps
c-deps: ## Build all C/C++ dependencies (jemalloc, PROJ, GEOS).
ifeq ($(SKIP_GEOS),)
c-deps: $(C_DEPS_GO) | $(LIBGEOS)
else
c-deps: $(C_DEPS_GO)
endif

# Submodules ------------------------------------------------------------------
# The C dependencies live in git submodules; each build rule below initializes
# its own submodule on demand, so nothing extra needs to be done here.

.PHONY: submodules
submodules: ## Initialize the git submodules (jemalloc, PROJ, GEOS, krb5).
	@test -d .git && git submodule update --init --recursive || true

# jemalloc --------------------------------------------------------------------

$(JEMALLOC_DIR)/Makefile:
	@echo "configuring jemalloc"
	@test -e $(C_DEPS_DIR)/jemalloc/configure.ac || git submodule update --init c-deps/jemalloc
	@cd $(C_DEPS_DIR)/jemalloc && test -x configure || autoconf
	@mkdir -p $(@D)
	@cd $(JEMALLOC_DIR) && env je_cv_madv_free=no $(C_DEPS_DIR)/jemalloc/configure --enable-prof > configure.log
	@grep -q '^je_cv_madv_free=no$$' $(JEMALLOC_DIR)/config.log || \
	  { echo "NOTE: MADV_FREE was not disabled; see cockroachdb/cockroach#83790" >&2; }

$(LIBJEMALLOC): $(JEMALLOC_DIR)/Makefile
	@echo "building jemalloc"
	+@$(MAKE) -C $(JEMALLOC_DIR) build_lib_static -j$(or $(NCPUS),4) > /dev/null

# PROJ ------------------------------------------------------------------------

$(PROJ_DIR)/Makefile:
	@echo "configuring PROJ"
	@test -e $(C_DEPS_DIR)/proj/CMakeLists.txt || git submodule update --init c-deps/proj
	@mkdir -p $(@D)
	@cd $(PROJ_DIR) && cmake $(C_DEPS_DIR)/proj -DCMAKE_BUILD_TYPE=Release -DBUILD_LIBPROJ_SHARED=OFF > cmake.log

$(LIBPROJ): $(PROJ_DIR)/Makefile
	@echo "building PROJ"
	+@$(MAKE) -C $(PROJ_DIR) proj -j$(or $(NCPUS),4) > /dev/null

# GEOS (shared; loaded via dlopen at run time from ./lib next to the binary) --

$(GEOS_DIR)/Makefile:
	@echo "configuring GEOS"
	@test -e $(C_DEPS_DIR)/geos/CMakeLists.txt || git submodule update --init c-deps/geos
	@mkdir -p $(@D)
	@cd $(GEOS_DIR) && cmake $(C_DEPS_DIR)/geos -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS=-fPIC "-DCMAKE_CXX_FLAGS=-fPIC -include cstdint" > cmake.log
	@mkdir -p $(GEOS_DIR)/capi/geos
	@cp -f $(C_DEPS_DIR)/geos/include/geos/export.h $(GEOS_DIR)/capi/geos/ 2>/dev/null || true

$(LIBGEOS): $(GEOS_DIR)/Makefile
	@echo "building GEOS"
	+@$(MAKE) -C $(GEOS_DIR) geos_c -j$(or $(NCPUS),4) > /dev/null
	@mkdir -p lib
	@cp -Lf $(GEOS_DIR)/lib/libgeos.$(DYN_EXT) lib/ 2>/dev/null || \
	  cp -Lf $(GEOS_DIR)/lib/libgeos*.$(DYN_EXT).* $(CURDIR)/lib/
	@cp -Lf $(GEOS_DIR)/lib/libgeos_c.$(DYN_EXT) lib/

# Kerberos 5 (only built when GSS=1) -------------------------------------------

$(KRB5_DIR)/Makefile:
	@echo "configuring krb5"
	@test -e $(C_DEPS_DIR)/krb5/src/configure.in || git submodule update --init c-deps/krb5
	@cd $(C_DEPS_DIR)/krb5/src && test -x configure || autoreconf -Wno-obsolete
	@mkdir -p $(@D)
	@cd $(KRB5_DIR) && env -u CXXFLAGS CFLAGS="-fcommon" $(C_DEPS_DIR)/krb5/src/configure --enable-static --disable-shared > configure.log

$(LIBKRB5): $(KRB5_DIR)/Makefile
	@echo "building krb5"
	+@$(MAKE) -C $(KRB5_DIR) -j$(or $(NCPUS),4) > /dev/null

# Convenient names for maintainers.
.PHONY: libjemalloc libproj libgeos libkrb5
libjemalloc: $(LIBJEMALLOC)
libproj:     $(LIBPROJ)
libgeos:     $(LIBGEOS)
libkrb5:     $(LIBKRB5)

# ---------------------------------------------------------------------------
# Protobuf code generation
# ---------------------------------------------------------------------------
#
# The generated *.pb.go / *.pb.gw.go files are not checked into the tree; they
# are produced from the .proto sources using buf's embedded protoc together
# with the gogoroach and grpc-gateway plugins. `make build` (and friends)
# regenerate them whenever a .proto file changes.

GO_LIST_DIR := $(GO) list -m -f '{{.Dir}}'

GOGO_PATH       := $(shell $(GO_LIST_DIR) github.com/gogo/protobuf)
ERRORS_PATH     := $(shell $(GO_LIST_DIR) github.com/cockroachdb/errors)
PROMETHEUS_PATH := $(shell $(GO_LIST_DIR) github.com/prometheus/client_model)
GWAPI_PATH      := $(shell $(GO_LIST_DIR) github.com/grpc-ecosystem/grpc-gateway)/third_party/googleapis
RAFT_PATH       := $(shell $(GO_LIST_DIR) go.etcd.io/raft/v3)

GOGOPROTO_PROTO := $(GOGO_PATH)/gogoproto/gogo.proto
ERRORS_PROTO    := $(ERRORS_PATH)/errorspb/errors.proto

# Staging directory so that proto imports of the form "raft/v3/..." resolve
# from a single `-I` root (mirroring the old vendor/go.etcd.io layout). Must
# not live under a dot-prefixed directory; buf's file walker skips hidden dirs.
PROTO_STAGE := $(BIN_DIR)/protosrc

$(PROTO_STAGE)/raft/v3:
	@mkdir -p $(@D)
	@chmod -R u+w $@ 2>/dev/null || true
	@rm -rf $@
	@cp -rL $(RAFT_PATH) $@
	@chmod -R u+w $@
	@find $@ -name '*.pb.go' -delete

GO_PROTOS := $(sort $(shell find pkg -type f -name '*.proto'))

GW_SERVER_PROTOS := ./pkg/server/serverpb/admin.proto ./pkg/server/serverpb/status.proto ./pkg/server/serverpb/authentication.proto
GW_TS_PROTOS     := ./pkg/ts/tspb/timeseries.proto
GW_PROTOS        := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)

# Map protobuf includes to the Go package containing the generated Go code.
# (Each line must extend the previous one with NO whitespace anywhere.)
PROTO_MAPPINGS := Mgoogle/api/annotations.proto=google.golang.org/genproto/googleapis/api/annotations,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,

PROTO_INCLUDES := \
	-Ipkg \
	-I$(GOGO_PATH) \
	-I$(PROTO_STAGE) \
	-I$(PROMETHEUS_PATH) \
	-I$(GWAPI_PATH) \
	-I$(ERRORS_PATH)


$(BIN_DIR)/buf:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ github.com/bufbuild/buf/cmd/buf

$(BIN_DIR)/protoc-gen-gogoroach:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ ./pkg/cmd/protoc-gen-gogoroach

$(BIN_DIR)/protoc-gen-grpc-gateway:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

$(bin_goimports):
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ golang.org/x/tools/cmd/goimports

PROTO_BIN := $(BIN_DIR)/buf $(BIN_DIR)/protoc-gen-gogoroach $(BIN_DIR)/protoc-gen-grpc-gateway $(BIN_DIR)/goimports

.PHONY: protobuf
protobuf: ## Regenerate Go code from protobuf definitions.
protobuf: $(BUILD_DIR)/.go_protobuf_sources $(BUILD_DIR)/.gw_protobuf_sources

$(BUILD_DIR)/.go_protobuf_sources: $(GO_PROTOS) $(GOGOPROTO_PROTO) $(ERRORS_PROTO) $(PROTO_STAGE)/raft/v3 $(PROTO_BIN)
	@echo "generating Go protobufs"
	@find pkg -name '*.pb.go' -delete
	set -e; for dir in $(sort $(dir $(GO_PROTOS))); do \
	  PATH=$(BIN_DIR):$$PATH $(BIN_DIR)/buf protoc $(PROTO_INCLUDES) \
	    --gogoroach_out=$(PROTO_MAPPINGS)plugins=grpc,import_prefix=github.com/cockroachdb/cockroach/pkg/,paths=source_relative:./pkg \
	    $$dir/*.proto; \
	done
	@find pkg -name '*.pb.go' -print0 | xargs -0r $(GOFMT) -s -w > /dev/null
	@touch $@

$(BUILD_DIR)/.gw_protobuf_sources: $(GW_PROTOS) $(GO_PROTOS) $(BUILD_DIR)/.go_protobuf_sources $(BIN_DIR)/protoc-gen-grpc-gateway $(BIN_DIR)/goimports $(PROTO_STAGE)/raft/v3
	@echo "generating gRPC gateway protobufs"
	@find pkg -name '*.pb.gw.go' -delete
	set -e; PATH=$(BIN_DIR):$$PATH $(BIN_DIR)/buf protoc $(PROTO_INCLUDES) \
	  --grpc-gateway_out=logtostderr=true,request_context=true,paths=source_relative:./pkg $(GW_SERVER_PROTOS);
	set -e; PATH=$(BIN_DIR):$$PATH $(BIN_DIR)/buf protoc $(PROTO_INCLUDES) \
	  --grpc-gateway_out=logtostderr=true,request_context=true,paths=source_relative:./pkg $(GW_TS_PROTOS)
	@find pkg -name '*.pb.gw.go' -print0 | xargs -0r $(BIN_DIR)/goimports -w
	@touch $@

PROTOBUF_STAMPS := $(BUILD_DIR)/.go_protobuf_sources $(BUILD_DIR)/.gw_protobuf_sources

# ---------------------------------------------------------------------------
# Other generated Go code
# ---------------------------------------------------------------------------
#
# The SQL parser (goyacc), opt (optgen) and embedded-certificate (go-bindata)
# sources are not checked into the tree either; they are generated with the
# tools below. Vectorized-execution (*.eg.go) and log-channel files *are*
# checked in and need no generation.

bin_goyacc := $(BIN_DIR)/goyacc
bin_goimports := $(BIN_DIR)/goimports
bin_stringer := $(BIN_DIR)/stringer
bin_optgen := $(BIN_DIR)/optgen
bin_bindata := $(BIN_DIR)/go-bindata
bin_terraformgen := $(BIN_DIR)/terraformgen

$(BIN_DIR)/goyacc:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ golang.org/x/tools/cmd/goyacc

$(BIN_DIR)/stringer:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ github.com/cockroachdb/tools/cmd/stringer

$(BIN_DIR)/optgen:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ ./pkg/sql/opt/optgen/cmd/optgen

$(BIN_DIR)/go-bindata:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ github.com/kevinburke/go-bindata/go-bindata

$(BIN_DIR)/terraformgen:
	@mkdir -p $(@D)
	$(GO) build -mod=mod -o $@ ./pkg/roachprod/vm/aws/terraformgen

GEN_TOOLS := $(bin_goyacc) $(bin_goimports) $(bin_stringer) $(bin_optgen) $(bin_bindata) $(bin_terraformgen)

# SQL parser ------------------------------------------------------------------

SQLPARSER_TARGETS = \
	pkg/sql/parser/sql.go \
	pkg/sql/parser/helpmap_test.go \
	pkg/sql/parser/help_messages.go \
	pkg/sql/lexbase/tokens.go \
	pkg/sql/lexbase/keywords.go \
	pkg/sql/lexbase/reserved_keywords.go \
	pkg/sql/pgrepl/pgreplparser/pgrepl.go \
	pkg/sql/plpgsql/parser/plpgsql.go \
	pkg/sql/plpgsql/parser/lexbase/tokens.go \
	pkg/sql/plpgsql/parser/lexbase/keywords.go \
	pkg/sql/scanner/token_names_test.go

.SECONDARY: pkg/sql/parser/gen/sql.go.tmp
pkg/sql/parser/gen/sql.go.tmp: pkg/sql/parser/gen/sql-gen.y $(GEN_TOOLS)
	set -euo pipefail; \
	PATH=$(BIN_DIR):$$PATH; \
	ret=$$(cd pkg/sql/parser/gen && goyacc -p sql -o sql.go.tmp sql-gen.y); \
	if expr "$$ret" : ".*conflicts" >/dev/null; then \
	  echo "$$ret"; exit 1; \
	fi

.SECONDARY: pkg/sql/plpgsql/parser/gen/plpgsql.go.tmp
pkg/sql/plpgsql/parser/gen/plpgsql.go.tmp: pkg/sql/plpgsql/parser/gen/plpgsql-gen.y $(GEN_TOOLS)
	set -euo pipefail; \
	PATH=$(BIN_DIR):$$PATH; \
	ret=$$(cd pkg/sql/plpgsql/parser/gen && goyacc -p plpgsql -o plpgsql.go.tmp plpgsql-gen.y); \
	if expr "$$ret" : ".*conflicts" >/dev/null; then \
	  echo "$$ret"; exit 1; \
	fi

.SECONDARY: pkg/sql/pgrepl/pgreplparser/gen/pgrepl.go.tmp
pkg/sql/pgrepl/pgreplparser/gen/pgrepl.go.tmp: pkg/sql/pgrepl/pgreplparser/gen/pgrepl-gen.y $(GEN_TOOLS)
	set -euo pipefail; \
	PATH=$(BIN_DIR):$$PATH; \
	ret=$$(cd pkg/sql/pgrepl/pgreplparser/gen && goyacc -p pgrepl -o pgrepl.go.tmp pgrepl-gen.y); \
	if expr "$$ret" : ".*conflicts" >/dev/null; then \
	  echo "$$ret"; exit 1; \
	fi

pkg/sql/scanner/token_names_test.go: pkg/sql/parser/gen/sql.go.tmp
	(echo "// Code generated by make. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 echo; \
	 echo "package scanner"; \
	 echo; \
	 echo "var tokenNames = map[int]string{"; \
	 grep '^const [A-Z][_A-Z0-9]* ' $^ | \
	 awk '{printf("%d: \"%s\",\n", $$4, $$2)}' && \
	 echo "}" )> $@.tmp || rm $@.tmp
	mv -f $@.tmp $@

pkg/sql/lexbase/tokens.go: pkg/sql/parser/gen/sql.go.tmp
	(echo "// Code generated by make. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 echo; \
	 echo "package lexbase"; \
	 echo; \
	 grep '^const [A-Z][_A-Z0-9]* ' $^) > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@

pkg/sql/parser/sql.go: pkg/sql/parser/gen/sql.go.tmp pkg/sql/lexbase/tokens.go pkg/sql/lexbase/keywords.go pkg/sql/lexbase/reserved_keywords.go $(GEN_TOOLS)
	(echo "// Code generated by goyacc. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 cat $< | \
	 sed -E 's/^const ([A-Z][_A-Z0-9]*) =.*$$/const \1 = lexbase.\1/g') > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	PATH=$(BIN_DIR):$$PATH goimports -w $@

pkg/sql/plpgsql/parser/lexbase/tokens.go: pkg/sql/plpgsql/parser/gen/plpgsql.go.tmp
	(echo "// Code generated by make. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 echo; \
	 echo "package lexbase"; \
	 echo; \
	 grep '^const [A-Z][_A-Z0-9]* ' $^) > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@

pkg/sql/plpgsql/parser/plpgsql.go: pkg/sql/plpgsql/parser/gen/plpgsql.go.tmp pkg/sql/plpgsql/parser/lexbase/tokens.go pkg/sql/plpgsql/parser/lexbase/keywords.go $(GEN_TOOLS)
	(echo "// Code generated by goyacc. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 cat $< | \
	 sed -E 's/^const ([A-Z][_A-Z0-9]*) =.*$$/const \1 = lexbase.\1/g') > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	PATH=$(BIN_DIR):$$PATH goimports -w $@

pkg/sql/pgrepl/pgreplparser/pgrepl.go: pkg/sql/pgrepl/pgreplparser/gen/pgrepl.go.tmp $(GEN_TOOLS)
	(echo "// Code generated by goyacc. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 cat $<) > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	PATH=$(BIN_DIR):$$PATH goimports -w $@

# Grammar preprocessing (types and help rules).
.SECONDARY: pkg/sql/parser/gen/sql-gen.y
pkg/sql/parser/gen/sql-gen.y: pkg/sql/parser/sql.y pkg/sql/parser/replace_help_rules.awk
	mkdir -p pkg/sql/parser/gen
	set -euo pipefail; \
	awk '/func.*sqlSymUnion/ {print $$(NF - 1)}' pkg/sql/parser/sql.y | \
	sed -e 's/[]\/$$*.^|[]/\\&/g' | \
	sed -e "s/^/s_(type|token) <(/" | \
	awk '{print $$0")>_\\1 <union> /* <\\2> */_"}' > pkg/sql/parser/gen/types_regex.tmp; \
	sed -E -f pkg/sql/parser/gen/types_regex.tmp < pkg/sql/parser/sql.y | \
	awk -f pkg/sql/parser/replace_help_rules.awk | \
	sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	rm pkg/sql/parser/gen/types_regex.tmp

.SECONDARY: pkg/sql/plpgsql/parser/gen/plpgsql-gen.y
pkg/sql/plpgsql/parser/gen/plpgsql-gen.y: pkg/sql/plpgsql/parser/plpgsql.y
	mkdir -p pkg/sql/plpgsql/parser/gen
	set -euo pipefail; \
	awk '/func.*plpgsqlSymUnion/ {print $$(NF - 1)}' pkg/sql/plpgsql/parser/plpgsql.y | \
	sed -e 's/[]\/$$*.^|[]/\\&/g' | \
	sed -e "s/^/s_(type|token) <(/" | \
	awk '{print $$0")>_\\1 <union> /* <\\2> */_"}' > pkg/sql/plpgsql/parser/gen/types_regex.tmp; \
	sed -E -f pkg/sql/plpgsql/parser/gen/types_regex.tmp < pkg/sql/plpgsql/parser/plpgsql.y | \
	sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	rm pkg/sql/plpgsql/parser/gen/types_regex.tmp

.SECONDARY: pkg/sql/pgrepl/pgreplparser/gen/pgrepl-gen.y
pkg/sql/pgrepl/pgreplparser/gen/pgrepl-gen.y: pkg/sql/pgrepl/pgreplparser/pgrepl.y
	mkdir -p pkg/sql/pgrepl/pgreplparser/gen
	set -euo pipefail; \
	awk '/func.*pgreplSymUnion/ {print $$(NF - 1)}' pkg/sql/pgrepl/pgreplparser/pgrepl.y | \
	sed -e 's/[]\/$$*.^|[]/\\&/g' | \
	sed -e "s/^/s_(type|token) <(/" | \
	awk '{print $$0")>_\\1 <union> /* <\\2> */_"}' > pkg/sql/pgrepl/pgreplparser/gen/types_regex.tmp; \
	sed -E -f pkg/sql/pgrepl/pgreplparser/gen/types_regex.tmp < pkg/sql/pgrepl/pgreplparser/pgrepl.y | \
	sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	rm pkg/sql/pgrepl/pgreplparser/gen/types_regex.tmp

pkg/sql/lexbase/reserved_keywords.go: pkg/sql/parser/sql.y pkg/sql/parser/reserved_keywords.awk
	awk -f pkg/sql/parser/reserved_keywords.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	$(GOFMT) -s -w $@

pkg/sql/lexbase/keywords.go: pkg/sql/parser/sql.y pkg/sql/lexbase/allkeywords/main.go
	$(GO) run -tags all-keywords pkg/sql/lexbase/allkeywords/main.go < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	$(GOFMT) -s -w $@

pkg/sql/plpgsql/parser/lexbase/keywords.go: pkg/sql/plpgsql/parser/plpgsql.y pkg/sql/lexbase/allkeywords/main.go
	$(GO) run -tags all-keywords pkg/sql/lexbase/allkeywords/main.go < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	$(GOFMT) -s -w $@

pkg/sql/parser/helpmap_test.go: pkg/sql/parser/gen/sql-gen.y pkg/sql/parser/help_gen_test.sh $(GEN_TOOLS)
	@PATH=$(BIN_DIR):$$PATH pkg/sql/parser/help_gen_test.sh < $< >$@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	$(GOFMT) -s -w $@

pkg/sql/parser/help_messages.go: pkg/sql/parser/sql.y pkg/sql/parser/help.awk
	awk -f pkg/sql/parser/help.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	$(GOFMT) -s -w $@

# Optimizer (optgen) ----------------------------------------------------------

OPTGEN_TARGETS = \
	pkg/sql/opt/memo/expr.og.go \
	pkg/sql/opt/operator.og.go \
	pkg/sql/opt/xform/explorer.og.go \
	pkg/sql/opt/norm/factory.og.go \
	pkg/sql/opt/rule_name.og.go \
	pkg/sql/opt/rule_name_string.go \
	pkg/sql/opt/exec/factory.og.go \
	pkg/sql/opt/exec/explain/explain_factory.og.go \
	pkg/sql/opt/exec/explain/plan_gist_factory.og.go

optgen-defs := pkg/sql/opt/ops/*.opt
optgen-norm-rules := pkg/sql/opt/norm/rules/*.opt
optgen-xform-rules := pkg/sql/opt/xform/rules/*.opt
optgen-exec-defs := pkg/sql/opt/exec/factory.opt

pkg/sql/opt/memo/expr.og.go: $(optgen-defs) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ exprs $(optgen-defs)

pkg/sql/opt/operator.og.go: $(optgen-defs) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ ops $(optgen-defs)

pkg/sql/opt/rule_name.og.go: $(optgen-defs) $(optgen-norm-rules) $(optgen-xform-rules) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ rulenames $(optgen-defs) $(optgen-norm-rules) $(optgen-xform-rules)

pkg/sql/opt/rule_name_string.go: pkg/sql/opt/rule_name.go pkg/sql/opt/rule_name.og.go $(bin_stringer)
	PATH=$(BIN_DIR):$$PATH stringer -output=$@ -type=RuleName $(filter %.go,$^)

pkg/sql/opt/xform/explorer.og.go: $(optgen-defs) $(optgen-xform-rules) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ explorer $(optgen-defs) $(optgen-xform-rules)

pkg/sql/opt/norm/factory.og.go: $(optgen-defs) $(optgen-norm-rules) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ factory $(optgen-defs) $(optgen-norm-rules)

pkg/sql/opt/exec/factory.og.go: $(optgen-defs) $(optgen-exec-defs) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ execfactory $(optgen-exec-defs)

pkg/sql/opt/exec/explain/explain_factory.og.go: $(optgen-defs) $(optgen-exec-defs) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ execexplain $(optgen-exec-defs)

pkg/sql/opt/exec/explain/plan_gist_factory.og.go: $(optgen-defs) $(optgen-exec-defs) $(bin_optgen)
	$(BIN_DIR)/optgen -out $@ execplangist $(optgen-exec-defs)

# Embedded bindata (test certificates, terraform templates) -------------------

GENERATED_TARGETS = \
	pkg/roachprod/vm/aws/embedded.go \
	pkg/security/securitytest/embedded.go

pkg/roachprod/vm/aws/embedded.go: $(bin_bindata) $(bin_terraformgen) pkg/roachprod/vm/aws/config.json pkg/roachprod/vm/aws/old.json
	cd $(@D) && PATH=$(BIN_DIR):$$PATH $(GO) generate

pkg/security/securitytest/embedded.go: $(BIN_DIR)/go-bindata $(shell find pkg/security/securitytest/test_certs -type f -not -name README.md -not -name regenerate.sh 2>/dev/null)
	cd $(@D) && PATH=$(BIN_DIR):$$PATH $(GO) generate

ALL_GENERATED := $(PROTOBUF_STAMPS) $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(GENERATED_TARGETS)

# ---------------------------------------------------------------------------
# Go binaries
# ---------------------------------------------------------------------------

.DEFAULT_GOAL := build

.PHONY: build
build: | check-go
build: ## Build the `cockroach` binary (full edition, with CCL code).
build: $(COCKROACH)

ifeq ($(SKIP_GEOS),)
$(COCKROACH) $(COCKROACHOSS) $(COCKROACHSHORT): | $(LIBGEOS)
endif

$(COCKROACH): $(C_DEPS_GO) $(ZCGO_FILES) $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(GENERATED_TARGETS) $(PROTOBUF_STAMPS)
	@echo "go build -o $@ ./pkg/cmd/cockroach"
	@$(GO_BUILD) -o $@ ./pkg/cmd/cockroach

.PHONY: oss
oss: | check-go
oss: ## Build the pure-OSS `cockroachoss` binary.
oss: $(COCKROACHOSS)

$(COCKROACHOSS): $(C_DEPS_GO) $(ZCGO_FILES) $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(GENERATED_TARGETS) $(PROTOBUF_STAMPS)
	@echo "go build -o $@ ./pkg/cmd/cockroach-oss"
	@$(GO_BUILD) -o $@ ./pkg/cmd/cockroach-oss

.PHONY: short
short: | check-go
short: ## Build `cockroachshort` (stripped-down development binary).
short: $(COCKROACHSHORT)

$(COCKROACHSHORT): $(C_DEPS_GO) $(ZCGO_FILES) $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(GENERATED_TARGETS) $(PROTOBUF_STAMPS)
	@echo "go build -o $@ ./pkg/cmd/cockroach-short"
	@$(GO) build $(GOFLAGS) -tags '$(BUILD_TAGS) short' -ldflags '$(LDFLAGS)' -o $@ ./pkg/cmd/cockroach-short

.PHONY: install
install: ## Build and install the `cockroach` binary into $(bindir).
install: $(COCKROACH)
	@$(INSTALL) -m 755 -d $(DESTDIR)$(bindir)
	@$(INSTALL) -m 755 $(COCKROACH) $(DESTDIR)$(bindir)/cockroach
	@if test -d lib; then \
	  $(INSTALL) -m 755 -d $(DESTDIR)$(prefix)/lib/cockroach; \
	  $(INSTALL) -m 755 lib/libgeos*.$(DYN_EXT) $(DESTDIR)$(prefix)/lib/cockroach/ 2>/dev/null || true; \
	fi

.PHONY: run
run: ## Start a single-node insecure cluster with the locally built binary.
run: $(COCKROACH)
	$(COCKROACH) start-single-node --insecure

# ---------------------------------------------------------------------------
# Testing and verification
# ---------------------------------------------------------------------------

.PHONY: test
test: | check-go $(ALL_GENERATED)
test: ## Run Go tests, e.g. `make test PKG=./pkg/sql TESTS=TestParse`.
	$(GO) test $(GOFLAGS) -tags '$(TEST_TAGS)' -timeout $(TESTTIMEOUT) $(PKG) -run '$(TESTS)' $(TESTFLAGS)

.PHONY: race
race: | check-go $(ALL_GENERATED)
race: ## Run Go tests with the race detector enabled.
	$(GO) test $(GOFLAGS) -tags '$(TEST_TAGS)' -race -timeout $(RACETIMEOUT) $(PKG) -run '$(TESTS)' $(TESTFLAGS)

RACETIMEOUT ?= 45m

.PHONY: buildtests
buildtests: | check-go $(ALL_GENERATED)
buildtests: ## Compile (but do not run) every test binary; catches broken tests.
	$(GO) test $(GOFLAGS) -tags '$(TEST_TAGS)' -exec true -count=1 $(PKG)

.PHONY: vet
vet: | check-go $(ALL_GENERATED)
vet: ## Run `go vet` over the tree.
	$(GO) vet $(GOFLAGS) -tags '$(BUILD_TAGS)' $(PKG)

.PHONY: fmt
fmt: ## Reformat all tracked Go sources with gofmt.
	@git ls-files '*.go' | xargs -r $(GO) fmt > /dev/null

# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------
#
# All generated code is checked into the tree, so regeneration is only needed
# by maintainers changing grammars, annotated types, or mocks.

.PHONY: tools
tools: ## Build code-generation tools (goyacc, stringer, mockgen, goimports) into ./bin.
	@mkdir -p $(BIN_DIR)
	$(GO) build -mod=mod -o $(BIN_DIR)/goyacc golang.org/x/tools/cmd/goyacc
	$(GO) build -mod=mod -o $(BIN_DIR)/goimports golang.org/x/tools/cmd/goimports
	$(GO) build -mod=mod -o $(BIN_DIR)/stringer github.com/cockroachdb/tools/cmd/stringer
	$(GO) build -mod=mod -o $(BIN_DIR)/mockgen github.com/golang/mock/mockgen

.PHONY: generate
generate: ## Regenerate code from in-tree generators (`go generate ./pkg/...`).
generate: | tools
	@PATH=$(BIN_DIR):$(PATH) $(GO) generate $(GOFLAGS) -tags '$(TEST_TAGS)' $(PKG)

.PHONY: gencode
gencode: ## Regenerate all non-checked-in Go code (protos, parser, opt, bindata).
gencode: | $(ALL_GENERATED)

# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------

.PHONY: clean
clean: ## Remove built binaries and generated build files.
	rm -f $(COCKROACH) $(COCKROACHOSS) $(COCKROACHSHORT)
	rm -rf $(BIN_DIR)
	rm -f $(ZCGO_FILES)

.PHONY: cleangen
cleangen: ## Remove all generated (non-checked-in) Go code.
	find pkg -name '*.pb.go' -delete
	find pkg -name '*.pb.gw.go' -delete
	rm -f $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(GENERATED_TARGETS)
	rm -rf pkg/sql/parser/gen pkg/sql/plpgsql/parser/gen pkg/sql/pgrepl/pgreplparser/gen
	rm -rf $(BUILD_DIR)/.go_protobuf_sources $(BUILD_DIR)/.gw_protobuf_sources

.PHONY: distclean
distclean: ## Also remove the C/C++ dependency builds and the go build cache.
distclean: clean
	rm -rf $(BUILD_DIR) lib
	$(GO) clean -cache -testcache

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Print this message.
	@echo "Usage:"
	@echo "  make [target...] [VAR=foo VAR2=bar...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'
	@echo ""
	@echo "Useful variables:"
	@printf "  %-24s %s\n" \
		"PKG" "package selector for tests/vet (default: ./pkg/...)" \
		"TESTS" "regex of tests to run with make test (default: .)" \
		"STDMALLOC=1" "use the system allocator instead of jemalloc" \
		"GSS=1" "build and link Kerberos 5 support (Linux)" \
		"SKIP_GEOS=1" "skip building the GEOS shared library" \
		"GO=go" "select the Go toolchain"
	@echo ""
	@echo "Typical usage:"
	@printf "  %s\n" \
		"make build                                # ./cockroach" \
		"make STDMALLOC=1 build                    # faster link, system malloc" \
		"make test PKG=./pkg/sql                   # unit tests for one package" \
		"make test PKG=./pkg/sql TESTS=TestParse   # a single test" \
		"make run                                  # single-node insecure server"

$(BUILD_DIR):
	@mkdir -p $@
