// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
//HybridLogCheckpointTask.cs
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace FASTER.core
{
    /// <summary>
    /// This task is the base class for a checkpoint "backend", which decides how a captured version is
    /// persisted on disk.
    /// </summary>
    internal abstract class HybridLogCheckpointOrchestrationTask : ISynchronizationTask
    {
        private long lastVersion;
        private long lastCheckpointVersion = -1; // ✅ 记录上次处理的版本号
        private int checkpointAttemptCount = 0;  // ✅ 尝试次数统计
        private int checkpointSkipCount = 0;     // ✅ 跳过次数统计
        protected long lastScannedTailAddress = -1;

        /// <inheritdoc />
        public virtual void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    lastVersion = faster.systemState.Version;
                    if (faster._hybridLogCheckpoint.IsDefault())
                    {
                        faster._hybridLogCheckpointToken = Guid.NewGuid();
                        faster.InitializeHybridLogCheckpoint(faster._hybridLogCheckpointToken, next.Version);
                    }
                    faster._hybridLogCheckpoint.info.version = next.Version;
                    // ✅ 首次或“强制全量”时，从 BeginAddress 开始；否则可用增量起点
                    var firstOrFull = faster.lastScannedTailAddress < 0 || faster.ForceFullOnNextCheckpoint; // 后面第2处会加这个标志
                    faster._hybridLogCheckpoint.info.startLogicalAddress = firstOrFull
                        ? faster.hlog.BeginAddress
                        : faster.lastScannedTailAddress;
                    faster._hybridLogCheckpoint.info.beginAddress = faster.hlog.BeginAddress;
                    break;


                case Phase.IN_PROGRESS:
                    checkpointAttemptCount++;
                    if (lastCheckpointVersion == next.Version)
                    {
                        checkpointSkipCount++;
                        Console.WriteLine($"[CPR] Skipping checkpoint for duplicate version {next.Version} (Skipped {checkpointSkipCount}/{checkpointAttemptCount})");
                        return;
                    }
                    lastCheckpointVersion = next.Version;
                    Console.WriteLine($"[CPR] Checkpoint started for version {next.Version} (Attempt #{checkpointAttemptCount}, Skipped: {checkpointSkipCount})");
                    faster.CheckpointVersionShift(lastVersion, next.Version);
                    break;

                case Phase.WAIT_FLUSH:
                    faster._hybridLogCheckpoint.info.headAddress = faster.hlog.HeadAddress;
                    faster._hybridLogCheckpoint.info.nextVersion = next.Version;
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, faster);
                    faster.WriteHybridLogMetaInfo();
                    faster.lastVersion = lastVersion;
                    break;

                case Phase.REST:
                    faster._hybridLogCheckpoint.Dispose();
                    var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    faster.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                    faster.checkpointTcs = nextTcs;
                    Console.WriteLine($"[CPR] Final checkpoint stats: {checkpointAttemptCount} attempts, {checkpointSkipCount} skipped.");
                    break;
            }
        }

        protected static void CollectMetadata<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            var seg = faster.hlog.GetSegmentOffsets();
            if (seg != null)
            {
                faster._hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                Array.Copy(seg, faster._hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
            }

            lock (faster._activeSessions)
            {
                List<int> toDelete = null;
                foreach (var kvp in faster._activeSessions)
                {
                    kvp.Value.session.AtomicSwitch(next.Version - 1);
                    if (!kvp.Value.isActive)
                    {
                        toDelete ??= new();
                        toDelete.Add(kvp.Key);
                    }
                }
                if (toDelete != null)
                {
                    foreach (var key in toDelete)
                        faster._activeSessions.Remove(key);
                }
            }

            foreach (var item in faster.RecoverableSessions)
            {
                faster._hybridLogCheckpoint.info.checkpointTokens.TryAdd(item.Item1, (item.Item2, item.Item3));
            }
        }

        public virtual void GlobalAfterEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        public virtual void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
        {
            if (current.Phase != Phase.PERSISTENCE_CALLBACK) return;

            if (ctx is not null)
            {
                if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                {
                    faster.IssueCompletionCallback(ctx, fasterSession);
                    ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                }
            }

            faster.epoch.Mark(EpochPhaseIdx.CheckpointCompletionCallback, current.Version);
            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, current.Version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A FoldOver checkpoint persists a version by setting the read-only marker past the last entry of that
    /// version on the log and waiting until it is flushed to disk. It is simple and fast, but can result
    /// in garbage entries on the log, and a slower recovery of performance.
    /// </summary>
    internal sealed class FoldOverCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        private long capturedTailForThisCP = -1;
        private long requiredFlushAddress = -1;

        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            base.GlobalBeforeEnteringState(next, faster);

            if (next.Phase == Phase.IN_PROGRESS)
            {
                // 捕捉本次 CP 的 tail
                capturedTailForThisCP = faster.Log.TailAddress;

                bool fullScan = faster.ForceFullOnNextCheckpoint || faster.lastScannedTailAddress < 0;
                long scanFrom = fullScan ? faster.Log.BeginAddress : faster.lastScannedTailAddress;
                long scanTo   = capturedTailForThisCP;

                // ✅ 防止 scanFrom >= scanTo 导致 Partitioner.Create 抛异常
                if (scanFrom >= scanTo)
                {
                    Console.WriteLine($"[CPR-MEM] No records to scan (from {scanFrom} to {scanTo})");
                }
                else
                {
                    var sw = Stopwatch.StartNew();
                    int dop = Math.Min(Environment.ProcessorCount, 16);
                    int pageSizeLogical = (int)faster.hlog.GetPageSize();

                    var checkpointBag = new ConcurrentBag<(long logical, long physical)>();
                    var rangePartition = Partitioner.Create(scanFrom, scanTo, pageSizeLogical);

                    Parallel.ForEach(rangePartition, new ParallelOptions { MaxDegreeOfParallelism = dop }, slice =>
                    {
                        using var it = faster.Log.Scan(slice.Item1, slice.Item2);
                        while (it.GetNext(out var ri))
                        {
                            if (ri.Invalid || ri.Tombstone) continue;
                            if (!fullScan && !ri.IsInNewVersion) continue;

                            long logical = it.CurrentAddress;
                            long physical = faster.hlog.GetPhysicalAddress(logical);
                            checkpointBag.Add((logical, physical));
                        }
                    });

                    faster.checkpointBuffer.Clear();
                    foreach (var tup in checkpointBag)
                        faster.checkpointBuffer.Add(tup);

                    sw.Stop();
                    Console.WriteLine($"[CPR-MEM] Collected {faster.checkpointBuffer.Count} records " +
                                    $"(from {scanFrom} to {scanTo}) in {sw.ElapsedMilliseconds} ms (||={dop})");
                }

                // 推进增量起点
                faster.lastScannedTailAddress = capturedTailForThisCP;
            }

            if (next.Phase != Phase.WAIT_FLUSH) return;

            // Fold-over：推进 RO 到 Tail
            faster.hlog.ShiftReadOnlyToTail(out var _, out faster._hybridLogCheckpoint.flushedSemaphore);

            // 强一致 flush 目标
            requiredFlushAddress = capturedTailForThisCP;
            faster._hybridLogCheckpoint.info.finalLogicalAddress = requiredFlushAddress;

            // flush 结束后清空 buffer
            // （如果只是验证一致性，可以保留 buffer 作检查）
        }

        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;

                // 等待 flush 到 requiredFlushAddress
                if (faster.hlog.FlushedUntilAddress < requiredFlushAddress)
                {
                    if (valueTasks != null && s != null)
                        valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                    return;
                }

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                faster.GlobalStateMachineStep(current);
        }
    }


    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
/// <summary>
/// 改良版 Snapshot：在 IN_PROGRESS 扫描 live data，WAIT_FLUSH 按 page flush snapshot
/// </summary>
    internal sealed class SnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        private long capturedTailForThisCP = -1;
        private long requiredFlushAddress = -1;

        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    faster._lastSnapshotCheckpoint.Dispose();
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    break;

                case Phase.IN_PROGRESS:
                    // 捕捉 tail
                    capturedTailForThisCP = faster.Log.TailAddress;

                    // 全量或增量扫描 live data
                    bool fullScan = faster.ForceFullOnNextCheckpoint || faster.lastScannedTailAddress < 0;
                    long scanFrom = fullScan ? faster.Log.BeginAddress : faster.lastScannedTailAddress;
                    using (var it = faster.Log.Scan(scanFrom, capturedTailForThisCP))
                    {
                        int collected = 0;
                        while (it.GetNext(out var ri))
                        {
                            if (ri.Invalid || ri.Tombstone) continue;
                            if (!fullScan && !ri.IsInNewVersion) continue;
                            collected++;
                        }
                        Console.WriteLine($"[CPR-MEM] Collected {collected} records (from {scanFrom} to {capturedTailForThisCP})");
                    }

                    faster.lastScannedTailAddress = capturedTailForThisCP;
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, faster);

                    // 设置 flush 目标
                    faster._hybridLogCheckpoint.info.finalLogicalAddress = faster.hlog.GetTailAddress();
                    faster._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = faster._hybridLogCheckpoint.info.finalLogicalAddress;

                    // 初始化 snapshot 设备
                    faster._hybridLogCheckpoint.snapshotFileDevice =
                        faster.checkpointManager.GetSnapshotLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice =
                        faster.checkpointManager.GetSnapshotObjectLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileDevice.Initialize(faster.hlog.GetSegmentSize());
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                    faster._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress = faster.hlog.FlushedUntilAddress;

                    long startPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.snapshotStartFlushedLogicalAddress);
                    long endPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (faster._hybridLogCheckpoint.info.finalLogicalAddress > faster.hlog.GetStartLogicalAddress(endPage))
                        endPage++;

                    // 计算强一致需要刷到的地址
                    requiredFlushAddress = capturedTailForThisCP;

                    // 按页 flush snapshot 文件
                    faster.hlog.AsyncFlushPagesToDevice(
                        startPage,
                        endPage,
                        faster._hybridLogCheckpoint.info.finalLogicalAddress,
                        faster._hybridLogCheckpoint.info.startLogicalAddress,
                        faster._hybridLogCheckpoint.snapshotFileDevice,
                        faster._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out faster._hybridLogCheckpoint.flushedSemaphore,
                        faster.ThrottleCheckpointFlushDelayMs);

                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    faster._hybridLogCheckpoint.info.flushedLogicalAddress = faster.hlog.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._lastSnapshotCheckpoint = faster._hybridLogCheckpoint.Transfer();
                    break;

                default:
                    base.GlobalBeforeEnteringState(next, faster);
                    break;
            }
        }

        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;
                bool notify = s != null && s.CurrentCount > 0;
                notify = notify || !faster.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                // 检查是否已刷到 requiredFlushAddress
                if (faster.hlog.FlushedUntilAddress < requiredFlushAddress)
                    return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                faster.GlobalStateMachineStep(current);
        }
    }


    /// <summary>
    /// An Incremental Snapshot makes a copy of only changes that have happened since the last full Snapshot.
    /// </summary>
    internal sealed class IncrementalSnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            switch (next.Phase)
            {
                case Phase.PREPARE:
                    faster._hybridLogCheckpoint = faster._lastSnapshotCheckpoint;
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.prevVersion = next.Version;
                    break;

                case Phase.IN_PROGRESS:
                    base.GlobalBeforeEnteringState(next, faster);
                    break;

                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.info.finalLogicalAddress = faster.hlog.GetTailAddress();

                    if (faster._hybridLogCheckpoint.deltaLog == null)
                    {
                        faster._hybridLogCheckpoint.deltaFileDevice = faster.checkpointManager.GetDeltaLogDevice(faster._hybridLogCheckpointToken);
                        faster._hybridLogCheckpoint.deltaFileDevice.Initialize(-1);
                        faster._hybridLogCheckpoint.deltaLog = new DeltaLog(faster._hybridLogCheckpoint.deltaFileDevice, faster.hlog.LogPageSizeBits, -1);
                        faster._hybridLogCheckpoint.deltaLog.InitializeForWrites(faster.hlog.bufferPool);
                    }

                    // 原生增量快照：将 delta 区间写入 delta 设备
                    faster.hlog.AsyncFlushDeltaToDevice(
                        faster.hlog.FlushedUntilAddress,
                        faster._hybridLogCheckpoint.info.finalLogicalAddress,
                        faster._lastSnapshotCheckpoint.info.finalLogicalAddress,
                        faster._hybridLogCheckpoint.prevVersion,
                        faster._hybridLogCheckpoint.deltaLog,
                        out faster._hybridLogCheckpoint.flushedSemaphore,
                        faster.ThrottleCheckpointFlushDelayMs);
                    break;

                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, faster);
                    faster._hybridLogCheckpoint.info.deltaTailAddress = faster._hybridLogCheckpoint.deltaLog.TailAddress;
                    faster.WriteHybridLogIncrementalMetaInfo(faster._hybridLogCheckpoint.deltaLog);
                    faster._hybridLogCheckpoint.info.deltaTailAddress = faster._hybridLogCheckpoint.deltaLog.TailAddress;
                    faster._lastSnapshotCheckpoint = faster._hybridLogCheckpoint.Transfer();
                    faster._hybridLogCheckpoint.Dispose();
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.Phase != Phase.WAIT_FLUSH) return;

            if (ctx is null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !faster.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx is not null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.Version);
            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.Version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// HybridLog checkpoint state machine wrapper.
    /// </summary>
    internal class HybridLogCheckpointStateMachine : VersionChangeStateMachine
    {
        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine to use the given checkpoint backend (either fold-over or
        /// snapshot), drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public HybridLogCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1)
            : base(targetVersion, new VersionChangeTask(), checkpointBackend) { }

        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected HybridLogCheckpointStateMachine(long targetVersion, params ISynchronizationTask[] tasks)
            : base(targetVersion, tasks) { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    result.Phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    result.Phase = Phase.REST;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}
