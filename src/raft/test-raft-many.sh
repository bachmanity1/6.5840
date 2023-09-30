#!/bin/bash

N="${1:-10}"
TEST_NAME="${2:-}"
TEMP_FILE="test-output-$TEST_NAME"
[[ -n $TEST_NAME ]] && OPTS="-run $TEST_NAME" || TEMP_FILE="test-output-raft"
[[ -e $TEMP_FILE ]] && rm $TEMP_FILE

for ((i=1; i<=$N; i++))
do
    go test -race $OPTS >> $TEMP_FILE
done

FAILS=$(cat $TEMP_FILE | grep 'FAIL	6.5840/raft' | wc -l | sed -e 's/^[[:space:]]*//')
if [ "$FAILS" -gt 0 ]; then
    echo "FAILED: There are $FAILS fails in $N iterations of $TEST_NAME test"
else
    echo "PASSED: All $N iterations of $TEST_NAME test passed"
    rm $TEMP_FILE
fi
