#!/bin/bash

echo "Bash version ${BASH_VERSION}..."

pagesize=50

for i in {0..46..1}
do
	offset=$(( i * pagesize ))

	curl "https://8kdx6rx8h4.execute-api.us-east-1.amazonaws.com/prod/sucursales?offset=$offset&limit=50" -o sucursales/suc-$i.json
done