#!/bin/zsh
CURRENT_DIR=$1
rm -rf "${CURRENT_DIR}/genproto"
for dir in "${CURRENT_DIR}"/protos/*; do
  if [ -d "$dir" ]; then
    protoc -I="${dir}" -I="${CURRENT_DIR}/protos" --go_out="${CURRENT_DIR}" --go-grpc_out="${CURRENT_DIR}" "${dir}"/*.proto
  fi
done