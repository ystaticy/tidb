// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package refresher

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// Refresher provides methods to refresh stats info.
// NOTE: Refresher is not thread-safe.
type Refresher struct {
	// This context is used to cancel background tasks when the domain exits.
	ctx context.Context
	// This will be refreshed every time we rebuild the priority queue.
	autoAnalysisTimeWindow priorityqueue.AutoAnalysisTimeWindow

	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker

	// jobs is the priority queue of analysis jobs.
	jobs *priorityqueue.AnalysisPriorityQueue

	// worker is the worker that runs the analysis jobs.
	worker *worker

	// lastSeenPruneMode is the last seen value of the partition prune mode.
	// Used to detect changes in the partition prune mode.
	lastSeenPruneMode variable.PartitionPruneMode

	// lastSeenAutoAnalyzeRatio is the last seen value of the auto analyze ratio.
	// Used to detect changes in the auto analyze ratio.
	lastSeenAutoAnalyzeRatio float64
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	ctx context.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
) *Refresher {
	maxConcurrency := int(vardef.AutoAnalyzeConcurrency.Load())
	r := &Refresher{
		ctx:            ctx,
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		jobs:           priorityqueue.NewAnalysisPriorityQueue(statsHandle),
		worker:         NewWorker(statsHandle, sysProcTracker, maxConcurrency),
	}
	if ddlNotifier != nil {
		ddlNotifier.RegisterHandler(notifier.PriorityQueueHandlerID, r.jobs.HandleDDLEvent)
	}

	return r
}

// UpdateConcurrency updates the maximum concurrency for auto-analyze jobs
func (r *Refresher) UpdateConcurrency() {
	newConcurrency := int(vardef.AutoAnalyzeConcurrency.Load())
	r.worker.UpdateConcurrency(newConcurrency)
}

// AnalyzeHighestPriorityTables picks tables with the highest priority and analyzes them.
// Note: Make sure the session has the latest variable values.
// Usually, this is done by the caller through `util.CallWithSCtx`.
func (r *Refresher) AnalyzeHighestPriorityTables(sctx sessionctx.Context) bool {
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	err := r.setAutoAnalysisTimeWindow(parameters)
	if err != nil {
		statslogutil.StatsErrVerboseSampleLogger().Error("Set auto analyze time window failed", zap.Error(err))
		return false
	}
	if !r.isWithinTimeWindow() {
		return false
	}
	currentAutoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[vardef.TiDBAutoAnalyzeRatio])
	currentPruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	if !r.jobs.IsInitialized() {
		if err := r.jobs.Initialize(r.ctx); err != nil {
			statslogutil.StatsErrVerboseSampleLogger().Error("Failed to initialize the queue", zap.Error(err))
			return false
		}
		r.lastSeenAutoAnalyzeRatio = currentAutoAnalyzeRatio
		r.lastSeenPruneMode = currentPruneMode
	} else {
		// Only do this if the queue is already initialized.
		if currentAutoAnalyzeRatio != r.lastSeenAutoAnalyzeRatio || currentPruneMode != r.lastSeenPruneMode {
			r.lastSeenAutoAnalyzeRatio = currentAutoAnalyzeRatio
			r.lastSeenPruneMode = currentPruneMode
			err := r.jobs.Rebuild()
			if err != nil {
				statslogutil.StatsErrVerboseSampleLogger().Error("Failed to rebuild the queue", zap.Error(err))
				return false
			}
		}
	}

	// Update the concurrency to the latest value.
	r.UpdateConcurrency()
	// Check remaining concurrency.
	maxConcurrency := r.worker.GetMaxConcurrency()
	currentRunningJobs := r.worker.GetRunningJobs()
	remainConcurrency := maxConcurrency - len(currentRunningJobs)
	if remainConcurrency <= 0 {
		statslogutil.StatsSampleLogger().Info("No concurrency available")
		return false
	}

	analyzedCount := 0
	for analyzedCount < remainConcurrency {
		job, err := r.jobs.Pop()
		if err != nil {
			// No more jobs to analyze.
			if stderrors.Is(err, priorityqueue.ErrHeapIsEmpty) {
				break
			}
			intest.Assert(false, "Failed to pop job from the queue", zap.Error(err))
			statslogutil.StatsLogger().Error("Failed to pop job from the queue", zap.Error(err))
			return false
		}

		if _, isRunning := currentRunningJobs[job.GetTableID()]; isRunning {
			statslogutil.StatsLogger().Debug("Job already running, skipping", zap.Int64("tableID", job.GetTableID()))
			continue
		}
		if valid, failReason := job.ValidateAndPrepare(sctx); !valid {
			statslogutil.StatsSampleLogger().Info(
				"Table not ready for analysis",
				zap.String("reason", failReason),
				zap.Stringer("job", job),
			)
			continue
		}

		statslogutil.StatsLogger().Info("Auto analyze triggered", zap.Stringer("job", job))

		submitted := r.worker.SubmitJob(job)
		intest.Assert(submitted, "Failed to submit job unexpectedly. "+
			"This should not occur as the concurrency limit was checked prior to job submission. "+
			"Please investigate potential race conditions or inconsistencies in the concurrency management logic.")
		if submitted {
			statslogutil.StatsLogger().Debug("Job submitted successfully",
				zap.Stringer("job", job),
				zap.Int("remainConcurrency", remainConcurrency),
				zap.Int("currentRunningJobs", len(currentRunningJobs)),
				zap.Int("maxConcurrency", maxConcurrency),
				zap.Int("analyzedCount", analyzedCount),
			)
			analyzedCount++
		} else {
			statslogutil.StatsLogger().Warn("Failed to submit job",
				zap.Stringer("job", job),
				zap.Int("remainConcurrency", remainConcurrency),
				zap.Int("currentRunningJobs", len(currentRunningJobs)),
				zap.Int("maxConcurrency", maxConcurrency),
				zap.Int("analyzedCount", analyzedCount),
			)
		}
	}

	if analyzedCount > 0 {
		statslogutil.StatsLogger().Debug("Auto analyze jobs submitted successfully", zap.Int("submittedCount", analyzedCount))
		return true
	}

	statslogutil.StatsSampleLogger().Info("No tables to analyze")
	return false
}

// GetPriorityQueueSnapshot returns the stats priority queue.
func (r *Refresher) GetPriorityQueueSnapshot() (statstypes.PriorityQueueSnapshot, error) {
	return r.jobs.Snapshot()
}

func (r *Refresher) setAutoAnalysisTimeWindow(
	parameters map[string]string,
) error {
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[vardef.TiDBAutoAnalyzeStartTime],
		parameters[vardef.TiDBAutoAnalyzeEndTime],
	)
	if err != nil {
		return errors.Wrap(err, "parse auto analyze period failed")
	}
	r.autoAnalysisTimeWindow = priorityqueue.NewAutoAnalysisTimeWindow(start, end)
	return nil
}

// isWithinTimeWindow checks if the current time is within the auto analyze time window.
func (r *Refresher) isWithinTimeWindow() bool {
	return r.autoAnalysisTimeWindow.IsWithinTimeWindow(time.Now())
}

// WaitAutoAnalyzeFinishedForTest waits for the auto analyze job to be finished.
// Only used in the test.
func (r *Refresher) WaitAutoAnalyzeFinishedForTest() {
	r.worker.WaitAutoAnalyzeFinishedForTest()
}

// GetRunningJobs returns the currently running jobs.
// Only used in the test.
func (r *Refresher) GetRunningJobs() map[int64]struct{} {
	return r.worker.GetRunningJobs()
}

// ProcessDMLChangesForTest processes DML changes for the test.
// Only used in the test.
func (r *Refresher) ProcessDMLChangesForTest() {
	if r.jobs.IsInitialized() {
		r.jobs.ProcessDMLChanges()
	}
}

// RequeueMustRetryJobsForTest requeues must retry jobs for the test.
// Only used in the test.
func (r *Refresher) RequeueMustRetryJobsForTest() {
	r.jobs.RequeueMustRetryJobs()
}

// Len returns the length of the analysis job queue.
func (r *Refresher) Len() int {
	l, err := r.jobs.Len()
	intest.Assert(err == nil, "Failed to get the queue length")
	return l
}

// Close stops all running jobs and releases resources.
func (r *Refresher) Close() {
	r.worker.Stop()
	if r.jobs != nil {
		r.jobs.Close()
	}
}

// OnBecomeOwner is used to handle the event when the current TiDB instance becomes the stats owner.
func (*Refresher) OnBecomeOwner() {
	// No action is taken when becoming the stats owner.
	// Initialization of the Refresher can fail, so operations are deferred until the first auto-analyze check.
}

// OnRetireOwner is used to handle the event when the current TiDB instance retires from being the stats owner.
func (r *Refresher) OnRetireOwner() {
	// Theoretically we should stop the worker here, but stopping analysis jobs can be time-consuming.
	// To avoid blocking etcd leader re-election, we only close the priority queue.
	r.jobs.Close()
}
