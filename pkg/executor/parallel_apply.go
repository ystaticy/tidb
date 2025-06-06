// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/applycache"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type result struct {
	chk *chunk.Chunk
	err error
}

type outerRow struct {
	row      *chunk.Row
	selected bool // if this row is selected by the outer side
}

// ParallelNestedLoopApplyExec is the executor for apply.
type ParallelNestedLoopApplyExec struct {
	exec.BaseExecutor

	// outer-side fields
	outerExec   exec.Executor
	outerFilter expression.CNFExprs
	outerList   *chunk.List
	outer       bool

	// inner-side fields
	// use slices since the inner side is paralleled
	corCols       [][]*expression.CorrelatedColumn
	innerFilter   []expression.CNFExprs
	innerExecs    []exec.Executor
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerRow      []*chunk.Row
	hasMatch      []bool
	hasNull       []bool
	joiners       []join.Joiner

	// fields about concurrency control
	concurrency int
	started     uint32
	drained     uint32 // drained == true indicates there is no more data
	freeChkCh   chan *chunk.Chunk
	resultChkCh chan result
	outerRowCh  chan outerRow
	exit        chan struct{}
	workerWg    sync.WaitGroup
	notifyWg    sync.WaitGroup

	// fields about cache
	cache              *applycache.ApplyCache
	useCache           bool
	cacheHitCounter    int64
	cacheAccessCounter int64

	memTracker *memory.Tracker // track memory usage.
}

// Open implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Open(ctx context.Context) error {
	err := exec.Open(ctx, e.outerExec)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.outerList = chunk.NewList(exec.RetTypes(e.outerExec), e.InitCap(), e.MaxChunkSize())
	e.outerList.GetMemTracker().SetLabel(memory.LabelForOuterList)
	e.outerList.GetMemTracker().AttachTo(e.memTracker)

	e.innerList = make([]*chunk.List, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerSelected = make([][]bool, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.outerRow = make([]*chunk.Row, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	for i := range e.concurrency {
		e.innerChunk[i] = exec.TryNewCacheChunk(e.innerExecs[i])
		e.innerList[i] = chunk.NewList(exec.RetTypes(e.innerExecs[i]), e.InitCap(), e.MaxChunkSize())
		e.innerList[i].GetMemTracker().SetLabel(memory.LabelForInnerList)
		e.innerList[i].GetMemTracker().AttachTo(e.memTracker)
	}

	e.freeChkCh = make(chan *chunk.Chunk, e.concurrency)
	e.resultChkCh = make(chan result, e.concurrency+1) // innerWorkers + outerWorker
	e.outerRowCh = make(chan outerRow)
	e.exit = make(chan struct{})
	for range e.concurrency {
		e.freeChkCh <- exec.NewFirstChunk(e)
	}

	if e.useCache {
		if e.cache, err = applycache.NewApplyCache(e.Ctx()); err != nil {
			return err
		}
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// Next implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if atomic.LoadUint32(&e.drained) == 1 {
		req.Reset()
		return nil
	}

	if atomic.CompareAndSwapUint32(&e.started, 0, 1) {
		e.workerWg.Add(1)
		go e.outerWorker(ctx)
		for i := range e.concurrency {
			e.workerWg.Add(1)
			workID := i
			go e.innerWorker(ctx, workID)
		}
		e.notifyWg.Add(1)
		go e.notifyWorker(ctx)
	}
	result := <-e.resultChkCh
	if result.err != nil {
		return result.err
	}
	if result.chk == nil { // no more data
		req.Reset()
		atomic.StoreUint32(&e.drained, 1)
		return nil
	}
	req.SwapColumns(result.chk)
	e.freeChkCh <- result.chk
	return nil
}

// Close implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Close() error {
	e.memTracker = nil
	if atomic.LoadUint32(&e.started) == 1 {
		close(e.exit)
		e.notifyWg.Wait()
		e.started = 0
	}
	// Wait all workers to finish before Close() is called.
	// Otherwise we may got data race.
	err := exec.Close(e.outerExec)

	if e.RuntimeStats() != nil {
		runtimeStats := join.NewJoinRuntimeStats()
		if e.useCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.SetCacheInfo(true, hitRatio)
		} else {
			runtimeStats.SetCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", e.concurrency))
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}
	return err
}

// notifyWorker waits for all inner/outer-workers finishing and then put an empty
// chunk into the resultCh to notify the upper executor there is no more data.
func (e *ParallelNestedLoopApplyExec) notifyWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)
	e.workerWg.Wait()
	e.putResult(nil, nil)
}

func (e *ParallelNestedLoopApplyExec) outerWorker(ctx context.Context) {
	defer trace.StartRegion(ctx, "ParallelApplyOuterWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	var selected []bool
	var err error
	for {
		failpoint.Inject("parallelApplyOuterWorkerPanic", nil)
		chk := exec.TryNewCacheChunk(e.outerExec)
		if err := exec.Next(ctx, e.outerExec, chk); err != nil {
			e.putResult(nil, err)
			return
		}
		if chk.NumRows() == 0 {
			close(e.outerRowCh)
			return
		}
		e.outerList.Add(chk)
		outerIter := chunk.NewIterator4Chunk(chk)
		selected, err = expression.VectorizedFilter(e.Ctx().GetExprCtx().GetEvalCtx(), e.Ctx().GetSessionVars().EnableVectorizedExpression, e.outerFilter, outerIter, selected)
		if err != nil {
			e.putResult(nil, err)
			return
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			select {
			case e.outerRowCh <- outerRow{&row, selected[i]}:
			case <-e.exit:
				return
			}
		}
	}
}

func (e *ParallelNestedLoopApplyExec) innerWorker(ctx context.Context, id int) {
	defer trace.StartRegion(ctx, "ParallelApplyInnerWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-e.freeChkCh:
		case <-e.exit:
			return
		}
		failpoint.Inject("parallelApplyInnerWorkerPanic", nil)
		err := e.fillInnerChunk(ctx, id, chk)
		if err == nil && chk.NumRows() == 0 { // no more data, this goroutine can exit
			return
		}
		if e.putResult(chk, err) {
			return
		}
	}
}

func (e *ParallelNestedLoopApplyExec) putResult(chk *chunk.Chunk, err error) (exit bool) {
	select {
	case e.resultChkCh <- result{chk, err}:
		return false
	case <-e.exit:
		return true
	}
}

func (e *ParallelNestedLoopApplyExec) handleWorkerPanic(ctx context.Context, wg *sync.WaitGroup) {
	if r := recover(); r != nil {
		err := util.GetRecoverError(r)
		logutil.Logger(ctx).Error("parallel nested loop join worker panicked", zap.Error(err), zap.Stack("stack"))
		e.resultChkCh <- result{nil, err}
	}
	if wg != nil {
		wg.Done()
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *ParallelNestedLoopApplyExec) fetchAllInners(ctx context.Context, id int) (err error) {
	var key []byte
	for _, col := range e.corCols[id] {
		*col.Data = e.outerRow[id].GetDatum(col.Index, col.RetType)
		if e.useCache {
			key, err = codec.EncodeKey(e.Ctx().GetSessionVars().StmtCtx.TimeZone(), key, *col.Data)
			err = e.Ctx().GetSessionVars().StmtCtx.HandleError(err)
			if err != nil {
				return err
			}
		}
	}
	if e.useCache { // look up the cache
		atomic.AddInt64(&e.cacheAccessCounter, 1)
		failpoint.Inject("parallelApplyGetCachePanic", nil)
		value, err := e.cache.Get(key)
		if err != nil {
			return err
		}
		if value != nil {
			e.innerList[id] = value
			atomic.AddInt64(&e.cacheHitCounter, 1)
			return nil
		}
	}

	err = exec.Open(ctx, e.innerExecs[id])
	defer func() { terror.Log(exec.Close(e.innerExecs[id])) }()
	if err != nil {
		return err
	}

	if e.useCache {
		// create a new one in this case since it may be in the cache
		e.innerList[id] = chunk.NewList(exec.RetTypes(e.innerExecs[id]), e.InitCap(), e.MaxChunkSize())
	} else {
		e.innerList[id].Reset()
	}

	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := exec.Next(ctx, e.innerExecs[id], e.innerChunk[id])
		if err != nil {
			return err
		}
		if e.innerChunk[id].NumRows() == 0 {
			break
		}

		e.innerSelected[id], err = expression.VectorizedFilter(e.Ctx().GetExprCtx().GetEvalCtx(), e.Ctx().GetSessionVars().EnableVectorizedExpression, e.innerFilter[id], innerIter, e.innerSelected[id])
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[id][row.Idx()] {
				e.innerList[id].AppendRow(row)
			}
		}
	}

	if e.useCache { // update the cache
		failpoint.Inject("parallelApplySetCachePanic", nil)
		if _, err := e.cache.Set(key, e.innerList[id]); err != nil {
			return err
		}
	}
	return nil
}

func (e *ParallelNestedLoopApplyExec) fetchNextOuterRow(id int, req *chunk.Chunk) (row *chunk.Row, exit bool) {
	for {
		select {
		case outerRow, ok := <-e.outerRowCh:
			if !ok { // no more data
				return nil, false
			}
			if !outerRow.selected {
				if e.outer {
					e.joiners[id].OnMissMatch(false, *outerRow.row, req)
					if req.IsFull() {
						return nil, false
					}
				}
				continue // try the next outer row
			}
			return outerRow.row, false
		case <-e.exit:
			return nil, true
		}
	}
}

func (e *ParallelNestedLoopApplyExec) fillInnerChunk(ctx context.Context, id int, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter[id] == nil || e.innerIter[id].Current() == e.innerIter[id].End() {
			if e.outerRow[id] != nil && !e.hasMatch[id] {
				e.joiners[id].OnMissMatch(e.hasNull[id], *e.outerRow[id], req)
			}
			var exit bool
			e.outerRow[id], exit = e.fetchNextOuterRow(id, req)
			if exit || req.IsFull() || e.outerRow[id] == nil {
				return nil
			}

			e.hasMatch[id] = false
			e.hasNull[id] = false

			err = e.fetchAllInners(ctx, id)
			if err != nil {
				return err
			}
			e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
			e.innerIter[id].Begin()
		}

		matched, isNull, err := e.joiners[id].TryToMatchInners(*e.outerRow[id], e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
