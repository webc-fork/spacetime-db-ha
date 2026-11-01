#!/usr/bin/env bash

DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -ex

if [ ! -d "sh" ]; then
  ln -s "$HOME/.local/share/cargo_sh" sh
  ./sh/init.sh
fi
