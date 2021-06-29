#!/bin/bash


BUCKET_NAME=$1

IDX=0


IDX=$((IDX + 1))

echo "Should fail to do this run this since first part is too small."
cargo run -q -- $BUCKET_NAME tstfile${IDX} us-west-2 1234 40960


IDX=$((IDX + 1))
echo "Should succeed here since only one part even though its small"
cargo run -q -- $BUCKET_NAME tstfile${IDX} us-west-2 40960



IDX=$((IDX + 1))
echo "Should succeed here since first part is bigger"
cargo run -q -- $BUCKET_NAME tstfile${IDX} us-west-2 5242890 2242890