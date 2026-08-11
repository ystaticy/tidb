// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"google.golang.org/grpc"
)

type controller struct {
	pb.UnimplementedExternalWorkloadControllerServer

	mu  sync.Mutex
	out io.Writer
}

type logEntry struct {
	Level                  string `json:"level"`
	Time                   string `json:"time"`
	RPC                    string `json:"rpc"`
	KeyspaceID             uint32 `json:"keyspace_id,omitempty"`
	KeyspaceName           string `json:"keyspace_name,omitempty"`
	TiDBPool               string `json:"tidb_pool,omitempty"`
	TableID                int64  `json:"table_id,omitempty"`
	TTLJobEnable           *bool  `json:"ttl_job_enable,omitempty"`
	SafePoint              uint64 `json:"safe_point,omitempty"`
	GCLifeTime             int64  `json:"gc_life_time,omitempty"`
	CompletedJobCreateTime uint64 `json:"completed_job_create_time,omitempty"`
	TaskID                 uint64 `json:"task_id,omitempty"`
}

func (c *controller) log(rpc string, header *pb.RequestHeader, fill func(*logEntry)) {
	entry := logEntry{
		Level: "debug",
		Time:  time.Now().Format(time.RFC3339Nano),
		RPC:   rpc,
	}
	if header != nil {
		entry.KeyspaceID = header.GetKeyspaceId()
		entry.KeyspaceName = header.GetKeyspaceName()
		entry.TiDBPool = header.GetTidbPool()
	}
	if fill != nil {
		fill(&entry)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	_ = json.NewEncoder(c.out).Encode(entry)
}

func ok() *pb.Response {
	return &pb.Response{Error: &pb.Error{Type: pb.ErrorType_OK}}
}

func (c *controller) Ping(context.Context, *pb.PingRequest) (*pb.Response, error) {
	c.log("Ping", nil, nil)
	return ok(), nil
}

func (c *controller) RegisterGC(_ context.Context, req *pb.RegisterGCRequest) (*pb.Response, error) {
	c.log("RegisterGC", req.GetHeader(), nil)
	return ok(), nil
}

func (c *controller) RecycleGC(_ context.Context, req *pb.RecycleGCRequest) (*pb.Response, error) {
	c.log("RecycleGC", req.GetHeader(), func(entry *logEntry) {
		entry.SafePoint = req.GetSafePoint()
	})
	return ok(), nil
}

func (c *controller) RegisterGCV2(_ context.Context, req *pb.RegisterGCV2Request) (*pb.Response, error) {
	c.log("RegisterGCV2", req.GetHeader(), func(entry *logEntry) {
		entry.SafePoint = req.GetSafePoint()
		entry.GCLifeTime = req.GetGcLifeTime()
	})
	return ok(), nil
}

func (c *controller) RecycleGCV2(_ context.Context, req *pb.RecycleGCV2Request) (*pb.Response, error) {
	c.log("RecycleGCV2", req.GetHeader(), func(entry *logEntry) {
		entry.SafePoint = req.GetSafePoint()
	})
	return ok(), nil
}

func (c *controller) UpdateGCLifeTime(_ context.Context, req *pb.UpdateGCLifeTimeRequest) (*pb.Response, error) {
	c.log("UpdateGCLifeTime", req.GetHeader(), func(entry *logEntry) {
		entry.GCLifeTime = req.GetGcLifeTime()
	})
	return ok(), nil
}

func (c *controller) GetBgTaskConfig(_ context.Context, req *pb.GetBgTaskConfigRequest) (*pb.GetBgTaskConfigResponse, error) {
	c.log("GetBgTaskConfig", req.GetHeader(), nil)
	return &pb.GetBgTaskConfigResponse{
		Error:            &pb.Error{Type: pb.ErrorType_OK},
		WorkerCount:      1,
		AutoScaleEnabled: false,
	}, nil
}

func (c *controller) RegisterBgTask(_ context.Context, req *pb.RegisterBgTaskRequest) (*pb.Response, error) {
	c.log("RegisterBgTask", req.GetHeader(), nil)
	return ok(), nil
}

func (c *controller) RecycleBgTask(_ context.Context, req *pb.RecycleBgTaskRequest) (*pb.Response, error) {
	c.log("RecycleBgTask", req.GetHeader(), nil)
	return ok(), nil
}

func (c *controller) UpdateBgTaskExecID(_ context.Context, req *pb.UpdateBgTaskExecIDRequest) (*pb.Response, error) {
	c.log("UpdateBgTaskExecID", req.GetHeader(), nil)
	return ok(), nil
}

func (c *controller) RegisterRemoteQuery(_ context.Context, req *pb.RegisterRemoteQueryRequest) (*pb.Response, error) {
	c.log("RegisterRemoteQuery", req.GetHeader(), nil)
	return ok(), nil
}

func (c *controller) RegisterTTLTask(_ context.Context, req *pb.RegisterTTLTaskRequest) (*pb.Response, error) {
	c.log("RegisterTTLTask", req.GetHeader(), func(entry *logEntry) {
		ttlJobEnable := req.GetTtlJobEnable()
		entry.TableID = req.GetTableId()
		entry.TTLJobEnable = &ttlJobEnable
	})
	return ok(), nil
}

func (c *controller) DeleteTTLTableInfo(_ context.Context, req *pb.DeleteTTLTableInfoRequest) (*pb.Response, error) {
	c.log("DeleteTTLTableInfo", req.GetHeader(), func(entry *logEntry) {
		entry.TableID = req.GetTableId()
	})
	return ok(), nil
}

func (c *controller) RecycleTTLTask(_ context.Context, req *pb.RecycleTTLTaskRequest) (*pb.Response, error) {
	c.log("RecycleTTLTask", req.GetHeader(), func(entry *logEntry) {
		entry.CompletedJobCreateTime = req.GetCompletedJobCreateTime()
	})
	return ok(), nil
}

func (c *controller) UpdateTTLJobEnable(_ context.Context, req *pb.UpdateTTLJobEnableRequest) (*pb.Response, error) {
	c.log("UpdateTTLJobEnable", req.GetHeader(), func(entry *logEntry) {
		ttlJobEnable := req.GetTtlJobEnable()
		entry.TTLJobEnable = &ttlJobEnable
	})
	return ok(), nil
}

func (c *controller) RegisterAutoAnalyze(_ context.Context, req *pb.RegisterAutoAnalyzeRequest) (*pb.Response, error) {
	c.log("RegisterAutoAnalyze", req.GetHeader(), func(entry *logEntry) {
		entry.TaskID = req.GetTaskId()
	})
	return ok(), nil
}

func (c *controller) RecycleAutoAnalyze(_ context.Context, req *pb.RecycleAutoAnalyzeRequest) (*pb.Response, error) {
	c.log("RecycleAutoAnalyze", req.GetHeader(), func(entry *logEntry) {
		entry.TaskID = req.GetTaskId()
	})
	return ok(), nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "listen address")
	logFile := flag.String("log-file", "", "write JSON debug logs to this file instead of stdout")
	flag.Parse()

	var out io.Writer = os.Stdout
	var closer io.Closer
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open log file: %v\n", err)
			os.Exit(1)
		}
		out = f
		closer = f
	}
	if closer != nil {
		defer closer.Close()
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", *addr, err)
		os.Exit(1)
	}

	server := grpc.NewServer()
	pb.RegisterExternalWorkloadControllerServer(server, &controller{out: out})

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ln)
	}()

	fmt.Fprintf(os.Stderr, "mock external workload controller listening on %s\n", ln.Addr().String())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		fmt.Fprintf(os.Stderr, "mock external workload controller stopping after %s\n", sig)
		server.GracefulStop()
	case err := <-errCh:
		fmt.Fprintf(os.Stderr, "mock external workload controller stopped: %v\n", err)
		os.Exit(1)
	}
}
