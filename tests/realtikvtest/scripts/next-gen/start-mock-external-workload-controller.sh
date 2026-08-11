#! /usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

function find_available_port() {
    local port="$1"
    while [[ "${port}" -lt 65536 ]]; do
        if ! lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "${port}"
            return 0
        fi
        port=$((port + 1))
    done
    echo "no available port found from $1" >&2
    return 1
}

function wait_for_controller() {
    local addr="$1"
    local port="${addr##*:}"
    local pid="$2"
    local stderr_file="$3"

    for _ in {1..30}; do
        if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "${pid}" >/dev/null 2>&1; then
            echo "mock external workload controller exited before listening. Log tail:" >&2
            tail -200 "${stderr_file}" >&2 || true
            return 1
        fi
        sleep 1
    done

    echo "timed out waiting for mock external workload controller. Log tail:" >&2
    tail -200 "${stderr_file}" >&2 || true
    return 1
}

function main() {
    local self_dir
    self_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
    local repo_root
    repo_root="$(realpath "${self_dir}/../../../..")"
    cd "${repo_root}"

    local work_dir="${EXTERNAL_WORKLOAD_MOCK_WORKDIR:-}"
    if [[ -z "${work_dir}" ]]; then
        work_dir="$(mktemp -d "${TMPDIR:-/tmp}/tidb-external-workload-mock.XXXXXX")"
    else
        mkdir -p "${work_dir}"
        work_dir="$(realpath "${work_dir}")"
    fi

    local controller_addr="${EXTERNAL_WORKLOAD_CONTROLLER_ADDR:-}"
    if [[ -z "${controller_addr}" ]]; then
        local controller_port
        controller_port="$(find_available_port "${EXTERNAL_WORKLOAD_CONTROLLER_PORT:-19090}")"
        controller_addr="127.0.0.1:${controller_port}"
    fi
    controller_addr="${controller_addr#http://}"
    controller_addr="${controller_addr#https://}"

    local controller_bin="${work_dir}/mock-external-workload-controller"
    local controller_log="${EXTERNAL_WORKLOAD_CONTROLLER_LOG:-${work_dir}/external-workload-controller.jsonl}"
    local controller_stderr="${work_dir}/external-workload-controller.stderr"
    local controller_pid_file="${work_dir}/external-workload-controller.pid"

    echo "Building mock external workload controller"
    go build -o "${controller_bin}" ./tests/realtikvtest/scripts/next-gen/mock-external-workload-controller

    echo "Starting mock external workload controller at ${controller_addr}"
    nohup "${controller_bin}" --addr "${controller_addr}" --log-file "${controller_log}" > "${controller_stderr}" 2>&1 &
    local controller_pid="$!"
    echo "${controller_pid}" > "${controller_pid_file}"
    wait_for_controller "${controller_addr}" "${controller_pid}" "${controller_stderr}"

    cat <<EOF
Mock external workload controller started.

TiDB config:
[external-workload]
enable = true
role = "master"
tidb-pool = "starter-ttl-smoke-pool"
controller-addr = "http://${controller_addr}"

controller_addr=http://${controller_addr}
log_file=${controller_log}
stderr_file=${controller_stderr}
pid_file=${controller_pid_file}

Stop:
kill "\$(cat ${controller_pid_file})"
EOF
}

main "$@"
