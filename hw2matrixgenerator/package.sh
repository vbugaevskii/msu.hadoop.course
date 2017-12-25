#!/bin/bash

set -e

USERNAME="Бугаевский"

ARCHIVE="Task2-$USERNAME.zip"
WORK_DIR="tmp"

mkdir -p "$WORK_DIR/prog"
[[ -z "$_DONT_REMOVE_WORK_DIR" ]] && trap "rm -rf ${WORK_DIR}" EXIT

cp -r src pom.xml config.xml "$WORK_DIR/prog"
cp readme.txt "$WORK_DIR"
[[ -f "$ARCHIVE" ]] && rm "$ARCHIVE"

cd "$WORK_DIR" ; zip -r "../$ARCHIVE" * ; cd ..
