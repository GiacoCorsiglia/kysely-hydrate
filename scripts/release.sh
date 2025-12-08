#!/usr/bin/env sh
set -e
npm version "$1"
git push --follow-tags
