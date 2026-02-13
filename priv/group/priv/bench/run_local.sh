#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

mix deps.get --check 2>/dev/null || mix deps.get
mix compile

mix run -e 'GroupBench.main(["local"])'
