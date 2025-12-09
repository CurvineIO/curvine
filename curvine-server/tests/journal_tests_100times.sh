#!/bin/bash

# run_raft_test.sh - Run curvine-server journal_test::check_raft_state 100 times
# Usage: chmod +x run_raft_test.sh && ./run_raft_test.sh

set -e  # Exit on any error

TEST_CMD="cargo test --package curvine-server --test journal_test -- check_raft_state --exact --nocapture"
TOTAL_RUNS=100
SUCCESS_COUNT=0
FAIL_COUNT=0
START_TIME=$(date +%s)

echo "🚀 Starting $TOTAL_RUNS runs of: $TEST_CMD"
echo "📅 Started at: $(date)"
echo "----------------------------------------"

for i in $(seq 1 $TOTAL_RUNS); do
    echo "🔄 Run $i/$TOTAL_RUNS"
    if $TEST_CMD; then
        echo "✅ Run $i PASSED"
        ((SUCCESS_COUNT++))
    else
        echo "❌ Run $i FAILED"
        ((FAIL_COUNT++))
        echo "🔄 Continuing with next run..."
    fi
    echo "📊 Progress: $SUCCESS_COUNT success, $FAIL_COUNT fails"
    echo "----------------------------------------"
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "🎉 TEST SUMMARY"
echo "----------------------------------------"
echo "📈 Total runs: $TOTAL_RUNS"
echo "✅ Success: $SUCCESS_COUNT"
echo "❌ Failures: $FAIL_COUNT"
echo "📊 Success rate: $((SUCCESS_COUNT * 100 / TOTAL_RUNS))%"
echo "⏱️  Total time: ${DURATION}s"
echo "📅 Finished at: $(date)"
echo "----------------------------------------"

if [ $FAIL_COUNT -eq 0 ]; then
    echo "🎊 ALL TESTS PASSED! 🚀"
    exit 0
else
    echo "⚠️  $FAIL_COUNT failures detected. Check logs above."
    exit 1
fi
