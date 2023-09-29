#!/bin/bash

N="${1:-10}"
TYPE="${2:-}"
TEMP_FILE="raft_test_output"

for ((i=1; i<=$N; i++))
do
    go test -race -run $TYPE >> $TEMP_FILE
done

FAILS=$(cat $TEMP_FILE | grep FAIL: | wc -l | sed -e 's/^[[:space:]]*//')
if [ "$FAILS" -gt 0 ]; then
    echo "FAILED: There are $FAILS fails in $N iterations of $TYPE test"
else
    echo "PASSED: All $N iterations of $TYPE test passed"
fi

rm $TEMP_FILE