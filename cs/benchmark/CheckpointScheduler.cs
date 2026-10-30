// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.benchmark
{
    /// <summary>
    /// One checkpoint cycle: from the start of one checkpoint to the start of the next one.
    /// </summary>
    internal struct CheckpointCycle
    {
        public int Index;
        public double StartMs;
        public double IntervalMs;   // start-to-start
        public double DurationMs;   // initiate -> checkpoint durable
        public long Ops;            // operations completed during the cycle
        public long LogBytes;       // log tail growth during the cycle
        public bool Full;           // index + log checkpoint, rather than log-only
        public long ScanBytes;      // log that recovery from this checkpoint must replay

        public double Throughput => Ops / (IntervalMs / 1000);
    }

    /// <summary>
    /// Linear model of recovery time: a fixed cost (loading the index, etc.) plus a per-MB cost of replaying the log
    /// written since the last index checkpoint. The coefficients are hardware dependent; calibrate them with
    /// --recoverytest runs at different --fullevery settings.
    /// </summary>
    internal readonly struct RecoveryModel
    {
        public readonly double FixedMs, MsPerMB;

        public RecoveryModel(double fixedMs, double msPerMB)
        {
            FixedMs = fixedMs;
            MsPerMB = msPerMB;
        }

        public double PredictMs(long scanBytes) => FixedMs + MsPerMB * scanBytes / 1e6;
    }

    /// <summary>
    /// Decides which checkpoints also checkpoint the index (a "full" checkpoint). Index checkpoints are expensive,
    /// but bound how much log recovery must replay.
    /// </summary>
    internal interface IIndexCheckpointPolicy
    {
        string Name { get; }

        /// <summary>Whether the checkpoint about to start should include the index.</summary>
        /// <param name="checkpointNumber">0-based number of the checkpoint about to start</param>
        /// <param name="bytesSinceIndex">log written since the last index checkpoint started (or since the log began)</param>
        bool ShouldCheckpointIndex(int checkpointNumber, long bytesSinceIndex);
    }

    /// <summary>Baseline: log-only checkpoints; recovery replays the whole log.</summary>
    internal sealed class NoIndexCheckpointPolicy : IIndexCheckpointPolicy
    {
        public string Name => "none";
        public bool ShouldCheckpointIndex(int checkpointNumber, long bytesSinceIndex) => false;
    }

    /// <summary>Baseline: every Nth checkpoint (starting with the first) is a full checkpoint.</summary>
    internal sealed class EveryNthIndexCheckpointPolicy : IIndexCheckpointPolicy
    {
        readonly int n;

        public EveryNthIndexCheckpointPolicy(int n) => this.n = n;

        public string Name => $"every-{n}";
        public bool ShouldCheckpointIndex(int checkpointNumber, long bytesSinceIndex) => checkpointNumber % n == 0;
    }

    /// <summary>
    /// Checkpoints the index only when recovery from a log-only checkpoint would come close to the recovery time
    /// objective. Because the decision is driven by the log actually written, index checkpoints are taken more
    /// often under write-heavy workloads and less often under read-heavy ones.
    /// </summary>
    internal sealed class RtoIndexCheckpointPolicy : IIndexCheckpointPolicy
    {
        readonly RecoveryModel model;
        readonly double budgetMs;

        /// <param name="safety">fraction of the RTO to target, leaving headroom for model error and for log
        /// written while the checkpoint itself runs</param>
        public RtoIndexCheckpointPolicy(RecoveryModel model, double rtoMs, double safety)
        {
            this.model = model;
            budgetMs = rtoMs * safety;
        }

        public string Name => "rto";
        public bool ShouldCheckpointIndex(int checkpointNumber, long bytesSinceIndex) => model.PredictMs(bytesSinceIndex) > budgetMs;
    }

    internal static class CheckpointObjective
    {
        /// <summary>
        /// Expected useful throughput if failures arrive with the given mean time between failures. On a failure,
        /// everything after the last durable checkpoint is lost; on average that is half an interval plus the
        /// time it takes the in-flight checkpoint to become durable.
        /// </summary>
        public static double EffectiveThroughput(in CheckpointCycle cycle, double mtbfMs)
            => cycle.Throughput * Math.Max(0, 1 - (cycle.IntervalMs / 2 + cycle.DurationMs) / mtbfMs);
    }

    /// <summary>
    /// Decides how long to wait between checkpoints.
    /// </summary>
    internal interface ICheckpointPolicy
    {
        string Name { get; }

        /// <summary>Called when a checkpoint starts; returns the target interval (ms) until the next one starts.</summary>
        double NextIntervalMs();

        /// <summary>Called when the cycle started by the preceding <see cref="NextIntervalMs"/> ends.</summary>
        void OnCycleCompleted(in CheckpointCycle cycle);
    }

    /// <summary>
    /// Baseline: checkpoint every <c>intervalMs</c> regardless of workload.
    /// </summary>
    internal sealed class FixedIntervalPolicy : ICheckpointPolicy
    {
        readonly double intervalMs;

        public FixedIntervalPolicy(double intervalMs) => this.intervalMs = intervalMs;

        public string Name => $"fixed-{intervalMs}";
        public double NextIntervalMs() => intervalMs;
        public void OnCycleCompleted(in CheckpointCycle cycle) { }
    }

    /// <summary>
    /// Young/Daly first-order optimum T = sqrt(2 * C * MTBF), with the checkpoint cost C taken to be the
    /// (smoothed) observed checkpoint duration. This is the classical model, which assumes checkpoints block
    /// the application for their whole duration.
    /// </summary>
    internal sealed class YoungDalyPolicy : ICheckpointPolicy
    {
        const double kAlpha = 0.3;  // EWMA weight of the newest duration sample

        readonly double mtbfMs, minMs, maxMs;
        double costMs = -1;

        public YoungDalyPolicy(double mtbfMs, double minMs, double maxMs)
        {
            this.mtbfMs = mtbfMs;
            this.minMs = minMs;
            this.maxMs = maxMs;
        }

        public string Name => "youngdaly";

        public double NextIntervalMs()
        {
            if (costMs < 0)
                return minMs;   // no estimate yet: checkpoint soon to measure the cost
            return Math.Clamp(Math.Sqrt(2 * costMs * mtbfMs), minMs, maxMs);
        }

        public void OnCycleCompleted(in CheckpointCycle cycle)
            => costMs = costMs < 0 ? cycle.DurationMs : kAlpha * cycle.DurationMs + (1 - kAlpha) * costMs;
    }

    /// <summary>
    /// Sliding-window UCB bandit over a fixed set of candidate intervals. The reward of a cycle is its effective
    /// throughput (see <see cref="CheckpointObjective"/>), so the policy learns the real cost of checkpointing for
    /// the current workload rather than assuming one. Only the last <c>window</c> cycles are remembered, so the
    /// policy re-adapts when the workload changes.
    /// </summary>
    internal sealed class SlidingWindowUcbPolicy : ICheckpointPolicy
    {
        readonly double[] armsMs;
        readonly double mtbfMs, exploration;
        readonly int window;
        readonly Queue<(int arm, double reward)> history = new();
        double maxReward;
        int lastArm = -1, nextInitialArm;

        public SlidingWindowUcbPolicy(double[] armsMs, double mtbfMs, int window, double exploration)
        {
            this.armsMs = armsMs;
            this.mtbfMs = mtbfMs;
            this.window = window;
            this.exploration = exploration;
        }

        public string Name => "ucb";

        public double NextIntervalMs()
        {
            lastArm = SelectArm();
            return armsMs[lastArm];
        }

        int SelectArm()
        {
            // Play every arm once before trusting any estimate.
            if (nextInitialArm < armsMs.Length)
                return nextInitialArm++;

            var counts = new int[armsMs.Length];
            var sums = new double[armsMs.Length];
            foreach (var (arm, reward) in history)
            {
                counts[arm]++;
                sums[arm] += maxReward > 0 ? reward / maxReward : 0;    // normalize to [0, 1]
            }

            // An arm that fell out of the window gets replayed.
            for (int i = 0; i < armsMs.Length; i++)
                if (counts[i] == 0)
                    return i;

            var logN = Math.Log(history.Count);
            int best = 0;
            double bestScore = double.NegativeInfinity;
            for (int i = 0; i < armsMs.Length; i++)
            {
                var score = sums[i] / counts[i] + exploration * Math.Sqrt(logN / counts[i]);
                if (score > bestScore)
                {
                    bestScore = score;
                    best = i;
                }
            }
            return best;
        }

        public void OnCycleCompleted(in CheckpointCycle cycle)
        {
            if (lastArm < 0)
                return;
            var reward = CheckpointObjective.EffectiveThroughput(cycle, mtbfMs);
            maxReward = Math.Max(maxReward, reward);
            history.Enqueue((lastArm, reward));
            while (history.Count > window)
                history.Dequeue();
        }
    }

    /// <summary>
    /// Drives checkpoints according to an <see cref="ICheckpointPolicy"/> while the workload runs, and records a
    /// per-sample throughput timeline and per-cycle statistics as CSV.
    /// </summary>
    internal sealed class CheckpointScheduler<Key, Value>
    {
        const double kWarmupMs = 1000;  // run uncheckpointed for this long before the first checkpoint

        readonly FasterKV<Key, Value> store;
        readonly ICheckpointPolicy policy;
        readonly Func<long> opsDone;
        readonly CheckpointType checkpointType;
        readonly bool tryIncremental;
        readonly double mtbfMs;
        readonly int sampleMs;
        readonly IIndexCheckpointPolicy indexPolicy;
        readonly RecoveryModel recoveryModel;

        int checkpointsStarted;
        long lastFullStartTail = -1, lastStartTail = -1;

        public readonly List<CheckpointCycle> Cycles = new();

        public CheckpointScheduler(FasterKV<Key, Value> store, ICheckpointPolicy policy, IIndexCheckpointPolicy indexPolicy,
            RecoveryModel recoveryModel, Func<long> opsDone, CheckpointType checkpointType, bool tryIncremental, double mtbfMs, int sampleMs)
        {
            this.store = store;
            this.policy = policy;
            this.indexPolicy = indexPolicy;
            this.recoveryModel = recoveryModel;
            this.opsDone = opsDone;
            this.checkpointType = checkpointType;
            this.tryIncremental = tryIncremental;
            this.mtbfMs = mtbfMs;
            this.sampleMs = sampleMs;
        }

        public string IndexPolicyName => indexPolicy.Name;

        /// <summary>Predicted recovery time from each completed checkpoint, by the scheduler's recovery model.</summary>
        public IEnumerable<double> PredictedRecoveryMs => Cycles.Select(c => recoveryModel.PredictMs(c.ScanBytes));

        /// <summary>
        /// Approximate amount of log that recovery from the last checkpoint must replay: recovery restores the last
        /// index checkpoint and then scans the log from that point up to the last log checkpoint. With no index
        /// checkpoint it scans from the beginning of the log.
        /// </summary>
        public long RecoveryScanEstimateBytes
            => lastStartTail < 0 ? 0 : lastStartTail - (lastFullStartTail >= 0 ? lastFullStartTail : store.Log.BeginAddress);

        public void Run(Stopwatch swatch, double runMs, string resultPrefix)
        {
            using var timeline = new StreamWriter(resultPrefix + "_timeline.csv");
            timeline.WriteLine("t_sec,ops_per_sec,in_checkpoint,tail_address");

            Task checkpointTask = null;
            double checkpointStartMs = 0, nextStartMs = kWarmupMs;
            double lastSampleMs = 0;
            long lastSampleOps = opsDone();
            CheckpointCycle cycle = default;
            bool inCycle = false;

            double now;
            while ((now = swatch.Elapsed.TotalMilliseconds) < runMs)
            {
                if (checkpointTask is not null && checkpointTask.IsCompleted)
                {
                    checkpointTask.GetAwaiter().GetResult();    // surface checkpoint failures
                    checkpointTask = null;
                    cycle.DurationMs = now - checkpointStartMs;
                }

                if (now - lastSampleMs >= sampleMs)
                {
                    var ops = opsDone();
                    var opsPerSec = (ops - lastSampleOps) / ((now - lastSampleMs) / 1000);
                    timeline.WriteLine(Invariant($"{now / 1000:F3},{opsPerSec:F0},{(checkpointTask is null ? 0 : 1)},{store.Log.TailAddress}"));
                    lastSampleMs = now;
                    lastSampleOps = ops;
                }

                // A cycle cannot end before its checkpoint is durable, even if the target interval has passed.
                if (checkpointTask is null && now >= nextStartMs)
                {
                    if (inCycle)
                        CompleteCycle(ref cycle, now);

                    var tail = store.Log.TailAddress;
                    var bytesSinceIndex = tail - (lastFullStartTail >= 0 ? lastFullStartTail : store.Log.BeginAddress);
                    var full = indexPolicy.ShouldCheckpointIndex(checkpointsStarted, bytesSinceIndex);
                    if (full ? store.TryInitiateFullCheckpoint(out _, checkpointType)
                             : store.TryInitiateHybridLogCheckpoint(out _, checkpointType, tryIncremental))
                    {
                        checkpointsStarted++;
                        lastStartTail = tail;
                        if (full)
                            lastFullStartTail = tail;
                        checkpointStartMs = now;
                        checkpointTask = store.CompleteCheckpointAsync().AsTask();
                        // Ops and LogBytes hold the starting values until CompleteCycle turns them into deltas.
                        cycle = new CheckpointCycle
                        {
                            Index = Cycles.Count, StartMs = now, Ops = opsDone(), LogBytes = tail, Full = full,
                            ScanBytes = full ? 0 : bytesSinceIndex
                        };
                        inCycle = true;
                        nextStartMs = now + policy.NextIntervalMs();
                    }
                    else
                        inCycle = false;
                }

                Thread.Sleep(1);
            }

            // Do not let the store be disposed under an in-flight checkpoint.
            checkpointTask?.GetAwaiter().GetResult();

            WriteCycles(resultPrefix + "_cycles.csv");
        }

        void CompleteCycle(ref CheckpointCycle cycle, double now)
        {
            cycle.IntervalMs = now - cycle.StartMs;
            cycle.Ops = opsDone() - cycle.Ops;
            cycle.LogBytes = store.Log.TailAddress - cycle.LogBytes;
            Cycles.Add(cycle);
            policy.OnCycleCompleted(cycle);
        }

        void WriteCycles(string path)
        {
            using var writer = new StreamWriter(path);
            writer.WriteLine("index,start_sec,interval_ms,duration_ms,ops,throughput,log_bytes,effective_throughput,policy,full,index_policy,scan_mb,predicted_recovery_ms");
            foreach (var c in Cycles)
            {
                writer.WriteLine(Invariant($"{c.Index},{c.StartMs / 1000:F3},{c.IntervalMs:F1},{c.DurationMs:F1},{c.Ops},{c.Throughput:F0},{c.LogBytes},")
                    + Invariant($"{CheckpointObjective.EffectiveThroughput(c, mtbfMs):F0},{policy.Name},{(c.Full ? 1 : 0)},{indexPolicy.Name},")
                    + Invariant($"{c.ScanBytes / 1e6:F1},{recoveryModel.PredictMs(c.ScanBytes):F1}"));
            }
        }

        /// <summary>Interval-weighted effective throughput over all completed cycles.</summary>
        public double EffectiveThroughput()
        {
            var totalMs = Cycles.Sum(c => c.IntervalMs);
            return totalMs == 0 ? 0 : Cycles.Sum(c => CheckpointObjective.EffectiveThroughput(c, mtbfMs) * c.IntervalMs) / totalMs;
        }

        static string Invariant(FormattableString s) => s.ToString(CultureInfo.InvariantCulture);
    }
}
