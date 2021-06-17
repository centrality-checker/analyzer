#!/bin/sh

timestamp_to_delete="$1"

items=$(find ../storage/npm -type f)

temp="${TMPDIR:-/tmp}/replace_temp_file.$$"
IFS=$'\n'
for item in $items; do
  sed '/^$timestamp_to_delete\,/d' "$item" >"$temp" && mv "$temp" "$item"
done
