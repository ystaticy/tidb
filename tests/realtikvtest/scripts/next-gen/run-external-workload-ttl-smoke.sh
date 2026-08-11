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

controller_pid=""
runner_dir=""

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

function cleanup() {
    if [[ -n "${controller_pid:-}" ]]; then
        if kill -0 "${controller_pid}" >/dev/null 2>&1; then
            kill -TERM "${controller_pid}" >/dev/null 2>&1 || true
        fi
        wait "${controller_pid}" >/dev/null 2>&1 || true
        controller_pid=""
    fi
    if [[ -n "${runner_dir:-}" && -d "${runner_dir}" ]]; then
        rm -rf -- "${runner_dir}"
        runner_dir=""
    fi
}

function wait_for_controller() {
    local addr="$1"
    local port="${addr##*:}"
    local pid="$2"
    local stderr_file="$3"

    for _ in {1..90}; do
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

function require_log_rpc() {
    local log_file="$1"
    local rpc="$2"
    if ! grep -q "\"rpc\":\"${rpc}\"" "${log_file}"; then
        echo "missing ${rpc} in mock external workload controller log: ${log_file}" >&2
        echo "log tail:" >&2
        tail -200 "${log_file}" >&2 || true
        return 1
    fi
}

function print_rpc_count() {
    local log_file="$1"
    local rpc="$2"
    local count
    count="$(grep -c "\"rpc\":\"${rpc}\"" "${log_file}" || true)"
    printf "%-24s %s\n" "${rpc}" "${count}"
}

function write_sql_runner() {
    local repo_root="$1"
    runner_dir="$(mktemp -d "${repo_root}/tmp.external-workload-sqlrunner.XXXXXX")"

    cat > "${runner_dir}/main.go" <<'EOF_GO'
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := os.Getenv("TIDB_STARTER_TEST_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "TIDB_STARTER_TEST_DSN is not set")
		os.Exit(1)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open TiDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping TiDB: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "set global tidb_ttl_job_enable = ON")
	}()

	statements := []string{
		"drop database if exists ttl_external_worker_smoke",
		"create database ttl_external_worker_smoke",
		"set global tidb_ttl_job_enable = OFF",
		"set global tidb_ttl_job_enable = ON",
		"create table ttl_external_worker_smoke.t(id int primary key, created_at datetime, updated_at datetime) TTL = created_at + INTERVAL 1 DAY",
		"alter table ttl_external_worker_smoke.t TTL = updated_at + INTERVAL 2 DAY",
		"alter table ttl_external_worker_smoke.t TTL_ENABLE = 'OFF'",
		"alter table ttl_external_worker_smoke.t TTL_ENABLE = 'ON'",
		"truncate table ttl_external_worker_smoke.t",
		"alter table ttl_external_worker_smoke.t remove ttl",
		"drop table ttl_external_worker_smoke.t",
		"drop database ttl_external_worker_smoke",
	}

	for _, stmt := range statements {
		fmt.Printf("SQL> %s\n", stmt)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			fmt.Fprintf(os.Stderr, "execute SQL failed: %s: %v\n", stmt, err)
			os.Exit(1)
		}
	}
}
EOF_GO
}

function write_post_activate_script() {
    local repo_root="$1"
    local post_script="$2"
    local rel_runner_dir="${runner_dir#${repo_root}/}"

    cat > "${post_script}" <<EOF_SH
#! /usr/bin/env bash
set -euo pipefail
cd "${repo_root}"
go run "./${rel_runner_dir}"
EOF_SH
    chmod +x "${post_script}"
}

function main() {
    local self_dir
    self_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
    local repo_root
    repo_root="$(realpath "${self_dir}/../../../..")"
    cd "${repo_root}"

    local work_dir="${EXTERNAL_WORKLOAD_TTL_SMOKE_WORKDIR:-}"
    if [[ -z "${work_dir}" ]]; then
        work_dir="$(mktemp -d "${TMPDIR:-/tmp}/tidb-external-workload-ttl-smoke.XXXXXX")"
    else
        mkdir -p "${work_dir}"
        work_dir="$(realpath "${work_dir}")"
    fi

    local controller_port
    controller_port="$(find_available_port "${EXTERNAL_WORKLOAD_CONTROLLER_PORT:-19090}")"
    local controller_addr="127.0.0.1:${controller_port}"
    local controller_log="${work_dir}/external-workload-controller.jsonl"
    local controller_stderr="${work_dir}/external-workload-controller.stderr"
    local controller_bin="${work_dir}/mock-external-workload-controller"
    local post_script="${work_dir}/run-ttl-sql.sh"

    trap cleanup EXIT

    write_sql_runner "${repo_root}"
    write_post_activate_script "${repo_root}" "${post_script}"

    echo "Building mock external workload controller"
    go build -o "${controller_bin}" ./tests/realtikvtest/scripts/next-gen/mock-external-workload-controller

    echo "Starting mock external workload controller at ${controller_addr}"
    "${controller_bin}" --addr "${controller_addr}" --log-file "${controller_log}" > "${controller_stderr}" 2>&1 &
    controller_pid="$!"
    wait_for_controller "${controller_addr}" "${controller_pid}" "${controller_stderr}"

    echo "Starting next-gen starter TiDB and running TTL DDL smoke SQL"
    STARTER_EXTERNAL_WORKLOAD_CONTROLLER_ADDR="http://${controller_addr}" \
    STARTER_EXTERNAL_WORKLOAD_ROLE="${STARTER_EXTERNAL_WORKLOAD_ROLE:-master}" \
    STARTER_EXTERNAL_WORKLOAD_TIDB_POOL="${STARTER_EXTERNAL_WORKLOAD_TIDB_POOL:-starter-ttl-smoke-pool}" \
    STARTER_POST_ACTIVATE_SCRIPT="${post_script}" \
    STARTER_STANDBY_MODE="${STARTER_STANDBY_MODE:-0}" \
    STARTER_KEYSPACE_NAME="${STARTER_KEYSPACE_NAME:-SYSTEM}" \
    STARTER_KEYSPACE_OBSERVABILITY="${STARTER_KEYSPACE_OBSERVABILITY:-0}" \
    STARTER_RUN_EXIT_WAIT_TEST=0 \
        "${self_dir}/run-starter-tests-with-server.sh" startertest "${STARTER_SUITE_TIMEOUT:-40m}" -run '^$'

    require_log_rpc "${controller_log}" "Ping"
    require_log_rpc "${controller_log}" "UpdateTTLJobEnable"
    require_log_rpc "${controller_log}" "RegisterTTLTask"
    require_log_rpc "${controller_log}" "DeleteTTLTableInfo"

    echo "External workload controller RPC counts:"
    print_rpc_count "${controller_log}" "Ping"
    print_rpc_count "${controller_log}" "UpdateTTLJobEnable"
    print_rpc_count "${controller_log}" "RegisterTTLTask"
    print_rpc_count "${controller_log}" "DeleteTTLTableInfo"
    print_rpc_count "${controller_log}" "RecycleTTLTask"
    echo "Logs kept in ${work_dir}"
}

main "$@"
