#!/bin/bash
set -e
git submodule update --remote --merge
cd progressivis
git submodule init
git submodule update --remote --merge
cd ..
pip install ./progressivis
