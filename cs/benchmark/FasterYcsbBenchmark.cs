// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// Define below to enable continuous performance report for dashboard
// #define DASHBOARD

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.benchmark
{
    internal class FASTER_YcsbBenchmark
    {
        // Ensure sizes are aligned to chunk sizes
        static long InitCount;
        static long TxnCount;

        readonly TestLoader testLoader;
        readonly ManualResetEventSlim waiter = new();
        readonly int numaStyle;
        // Not readonly: the workload mix may be switched mid-run (see Options.ShiftSeconds).
        volatile int readPercent, upsertPercent, rmwPercent;
        readonly Functions functions;
        readonly Input[] input_;

        readonly Key[] init_keys_;
        readonly Key[] txn_keys_;

        // Not readonly: the recovery test replaces them with a freshly recovered store.
        IDevice device;
        FasterKV<Key, Value> store;

        long idx_ = 0;
        long total_ops_done = 0;
        volatile bool done = false;

        // Per-thread running op counts, published periodically so the checkpoint scheduler can observe live throughput.
        // Each thread's slot is a cache line apart to avoid false sharing.
        const int kProgressStride = 16;
        readonly long[] progress;

        internal FASTER_YcsbBenchmark(Key[] i_keys_, Key[] t_keys_, TestLoader testLoader)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // Affinize main thread to last core on first socket if not used by experiment
                var (numGrps, numProcs) = Native32.GetNumGroupsProcsPerGroup();
                if ((testLoader.Options.NumaStyle == 0 && testLoader.Options.ThreadCount <= (numProcs - 1)) ||
                    (testLoader.Options.NumaStyle == 1 && testLoader.Options.ThreadCount <= numGrps * (numProcs - 1)))
                    Native32.AffinitizeThreadRoundRobin(numProcs - 1);
            }
            this.testLoader = testLoader;
            init_keys_ = i_keys_;
            txn_keys_ = t_keys_;
            numaStyle = testLoader.Options.NumaStyle;
            readPercent = testLoader.ReadPercent;
            upsertPercent = testLoader.UpsertPercent;
            rmwPercent = testLoader.RmwPercent;
            progress = new long[testLoader.Options.ThreadCount * kProgressStride];
            functions = new Functions();

#if DASHBOARD
            statsWritten = new AutoResetEvent[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                statsWritten[i] = new AutoResetEvent(false);
            }
            threadThroughput = new double[threadCount];
            threadAverageLatency = new double[threadCount];
            threadMaximumLatency = new double[threadCount];
            threadProgress = new long[threadCount];
            writeStats = new bool[threadCount];
            freq = Stopwatch.Frequency;
#endif

            input_ = new Input[8];
            for (int i = 0; i < 8; i++)
                input_[i].value = i;

            // The recovery test reopens the log after the run, so it must survive the first store, and must not see
            // checkpoints left over from earlier runs.
            if (testLoader.Options.RecoveryTest && Directory.Exists(testLoader.CheckpointPath))
                Directory.Delete(testLoader.CheckpointPath, recursive: true);

            (device, store) = CreateStore(preallocateFile: true, deleteOnClose: !testLoader.RecoverMode && !testLoader.Options.RecoveryTest);
        }

        private (IDevice, FasterKV<Key, Value>) CreateStore(bool preallocateFile, bool deleteOnClose)
        {
            var device = Devices.CreateLogDevice(TestLoader.DevicePath, preallocateFile: preallocateFile, deleteOnClose: deleteOnClose, useIoCompletionPort: true);

            if (testLoader.Options.ThreadCount >= 16)
                device.ThrottleLimit = testLoader.Options.ThreadCount * 12;

            var logSettings = testLoader.Options.UseSmallMemoryLog
                ? new LogSettings { LogDevice = device, PreallocateLog = true, PageSizeBits = 25, SegmentSizeBits = 30, MemorySizeBits = 28 }
                : new LogSettings { LogDevice = device, PreallocateLog = true };
            var store = new FasterKV<Key, Value>(testLoader.MaxKey / testLoader.Options.HashPacking, logSettings,
                new CheckpointSettings { CheckpointDir = testLoader.CheckpointPath }, concurrencyControlMode: testLoader.ConcurrencyControlMode);
            return (device, store);
        }

        /// <summary>
        /// Simulates a crash after the run: drops the store without checkpointing again, then recovers a new store
        /// from the last checkpoint. Returns the recovery time in milliseconds.
        /// </summary>
        private double RunRecoveryTest()
        {
            store.Dispose();
            device.Dispose();

            (device, store) = CreateStore(preallocateFile: false, deleteOnClose: true);
            var sw = Stopwatch.StartNew();
            var version = store.Recover();
            sw.Stop();

            Console.WriteLine($"Recovered version {version} in {sw.Elapsed.TotalMilliseconds:N1} ms");
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.After, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));
            return sw.Elapsed.TotalMilliseconds;
        }

        internal void Dispose()
        {
            store.Dispose();
            device.Dispose();
        }

        private void RunYcsbUnsafeContext(int thread_idx)
        {
            RandomGenerator rng = new((uint)(1 + thread_idx));

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var sw = Stopwatch.StartNew();

            Value value = default;
            Input input = default;
            Output output = default;

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            var session = store.For(functions).NewSession<Functions>();
            var uContext = session.UnsafeContext;
            uContext.BeginUnsafe();

            try
            {
                while (!done)
                {
                    long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    while (chunk_idx >= TxnCount)
                    {
                        if (chunk_idx == TxnCount)
                            idx_ = 0;
                        chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    }

                    for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                    {
                        if (idx % 512 == 0)
                        {
                            uContext.Refresh();
                            uContext.CompletePending(false);
                            Volatile.Write(ref progress[thread_idx * kProgressStride], reads_done + writes_done + deletes_done);
                        }

                        int r = (int)rng.Generate(100);     // rng.Next() is not inclusive of the upper bound so this will be <= 99
                        if (r < readPercent)
                        {
                            uContext.Read(ref txn_keys_[idx], ref input, ref output, Empty.Default, 1);
                            ++reads_done;
                            continue;
                        }
                        if (r < upsertPercent)
                        {
                            uContext.Upsert(ref txn_keys_[idx], ref value, Empty.Default, 1);
                            ++writes_done;
                            continue;
                        }
                        if (r < rmwPercent)
                        {
                            uContext.RMW(ref txn_keys_[idx], ref input_[idx & 0x7], Empty.Default, 1);
                            ++writes_done;
                            continue;
                        }
                        uContext.Delete(ref txn_keys_[idx], Empty.Default, 1);
                        ++deletes_done;
                    }

#if DASHBOARD
                    count += (int)kChunkSize;

                    //Check if stats collector is requesting for statistics
                    if (writeStats[thread_idx])
                    {
                        var tstart1 = tstop1;
                        tstop1 = Stopwatch.GetTimestamp();
                        threadProgress[thread_idx] = count;
                        threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                        lastWrittenValue = count;
                        writeStats[thread_idx] = false;
                        statsWritten[thread_idx].Set();
                    }
#endif
                }

                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }

            session.Dispose();

            sw.Stop();

#if DASHBOARD
            statsWritten[thread_idx].Set();
#endif

            Console.WriteLine($"Thread {thread_idx} done; {reads_done} reads, {writes_done} writes, {deletes_done} deletes in {sw.ElapsedMilliseconds} ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done + deletes_done);
        }

        private void RunYcsbSafeContext(int thread_idx)
        {
            RandomGenerator rng = new((uint)(1 + thread_idx));

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var sw = Stopwatch.StartNew();

            Value value = default;
            Input input = default;
            Output output = default;

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

            var session = store.For(functions).NewSession<Functions>();

            while (!done)
            {
                long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                while (chunk_idx >= TxnCount)
                {
                    if (chunk_idx == TxnCount)
                        idx_ = 0;
                    chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                }

                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                {
                    if (idx % 512 == 0)
                    {
                        if (!testLoader.Options.UseSafeContext)
                            session.Refresh();
                        session.CompletePending(false);
                        Volatile.Write(ref progress[thread_idx * kProgressStride], reads_done + writes_done + deletes_done);
                    }

                    int r = (int)rng.Generate(100);     // rng.Next() is not inclusive of the upper bound so this will be <= 99
                    if (r < readPercent)
                    {
                        session.Read(ref txn_keys_[idx], ref input, ref output, Empty.Default, 1);
                        ++reads_done;
                        continue;
                    }
                    if (r < upsertPercent)
                    {
                        session.Upsert(ref txn_keys_[idx], ref value, Empty.Default, 1);
                        ++writes_done;
                        continue;
                    }
                    if (r < rmwPercent)
                    {
                        session.RMW(ref txn_keys_[idx], ref input_[idx & 0x7], Empty.Default, 1);
                        ++writes_done;
                        continue;
                    }
                    session.Delete(ref txn_keys_[idx], Empty.Default, 1);
                    ++deletes_done;
                }
            }

            session.CompletePending(true);
            session.Dispose();

            sw.Stop();

            Console.WriteLine($"Thread {thread_idx} done; {reads_done} reads, {writes_done} writes, {deletes_done} deletes in {sw.ElapsedMilliseconds} ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done);
        }

        internal unsafe (double, double) Run(TestLoader testLoader)
        {
#if DASHBOARD
            var dash = new Thread(() => DoContinuousMeasurements());
            dash.Start();
#endif

            Thread[] workers = new Thread[testLoader.Options.ThreadCount];

            Console.WriteLine("Executing setup.");

            var storeWasRecovered = testLoader.MaybeRecoverStore(store);
            long elapsedMs = 0;
            if (!storeWasRecovered)
            {
                // Setup the store for the YCSB benchmark.
                Console.WriteLine("Loading FasterKV from data");
                for (int idx = 0; idx < testLoader.Options.ThreadCount; ++idx)
                {
                    int x = idx;
                    if (testLoader.Options.UseSafeContext)
                        workers[idx] = new Thread(() => SetupYcsbSafeContext(x));
                    else
                        workers[idx] = new Thread(() => SetupYcsbUnsafeContext(x));
                }

                foreach (Thread worker in workers)
                {
                    worker.Start();
                }

                waiter.Set();
                var sw = Stopwatch.StartNew();
                foreach (Thread worker in workers)
                {
                    worker.Join();
                }
                sw.Stop();
                elapsedMs = sw.ElapsedMilliseconds;
                waiter.Reset();
            }
            double insertsPerSecond = elapsedMs == 0 ? 0 : ((double)InitCount / elapsedMs) * 1000;
            Console.WriteLine(TestStats.GetLoadingTimeLine(insertsPerSecond, elapsedMs));
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.Before, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            if (!storeWasRecovered)
                testLoader.MaybeCheckpointStore(store);

            // Uncomment below to dispose log from memory, use for 100% read workloads only
            // store.Log.DisposeFromMemory();

            idx_ = 0;

            if (testLoader.Options.DumpDistribution)
                Console.WriteLine(store.DumpDistribution());

            // Ensure first fold-over checkpoint is fast
            if (testLoader.Options.CheckpointingEnabled && testLoader.Options.PeriodicCheckpointType == CheckpointType.FoldOver)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            Console.WriteLine("Executing experiment.");

            // Run the experiment.
            for (int idx = 0; idx < testLoader.Options.ThreadCount; ++idx)
            {
                int x = idx;
                if (testLoader.Options.UseSafeContext)
                    workers[idx] = new Thread(() => RunYcsbSafeContext(x));
                else
                    workers[idx] = new Thread(() => RunYcsbUnsafeContext(x));
            }
            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            waiter.Set();
            var swatch = Stopwatch.StartNew();

            using var shiftTimer = testLoader.Options.ShiftSeconds > 0
                ? new Timer(_ => ShiftWorkloadMix(), null, TimeSpan.FromSeconds(testLoader.Options.ShiftSeconds), Timeout.InfiniteTimeSpan)
                : null;

            CheckpointScheduler<Key, Value> scheduler = null;
            if (!testLoader.Options.CheckpointingEnabled)
            {
                Thread.Sleep(TimeSpan.FromSeconds(testLoader.Options.RunSeconds));
            }
            else
            {
                scheduler = CreateCheckpointScheduler();
                scheduler.Run(swatch, 1000.0 * testLoader.Options.RunSeconds, testLoader.Options.ResultPrefix);
                Console.WriteLine($"Checkpoint taken {scheduler.Cycles.Count}");
            }

            swatch.Stop();

            done = true;

            foreach (Thread worker in workers)
            {
                worker.Join();
            }

            waiter.Reset();

#if DASHBOARD
            dash.Join();
#endif

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.After, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            double opsPerSecond = total_ops_done / seconds;
            Console.WriteLine(TestStats.GetTotalOpsString(total_ops_done, seconds));
            Console.WriteLine(TestStats.GetStatsLine(StatsLineNum.Iteration, YcsbConstants.OpsPerSec, opsPerSecond));

            if (scheduler is not null)
            {
                var recoveryMs = testLoader.Options.RecoveryTest ? RunRecoveryTest() : -1;
                AppendCheckpointSummary(scheduler, opsPerSecond, recoveryMs);
            }
            return (insertsPerSecond, opsPerSecond);
        }

        private long OpsDoneSoFar()
        {
            long sum = 0;
            for (int i = 0; i < testLoader.Options.ThreadCount; i++)
                sum += Volatile.Read(ref progress[i * kProgressStride]);
            return sum;
        }

        private CheckpointScheduler<Key, Value> CreateCheckpointScheduler()
        {
            var options = testLoader.Options;
            double mtbfMs = 1000.0 * options.MtbfSeconds;
            var armsMs = options.UcbArmsMs.Select(x => (double)x).OrderBy(x => x).ToArray();

            ICheckpointPolicy policy = options.EffectiveCheckpointPolicy switch
            {
                "fixed" => new FixedIntervalPolicy(options.PeriodicCheckpointMilliseconds),
                "youngdaly" => new YoungDalyPolicy(mtbfMs, armsMs.First(), armsMs.Last()),
                "ucb" => new SlidingWindowUcbPolicy(armsMs, mtbfMs, options.UcbWindow, options.UcbExploration),
                _ => throw new InvalidOperationException($"Unknown checkpoint policy: {options.EffectiveCheckpointPolicy}")
            };

            var recoveryModel = new RecoveryModel(options.RecoveryFixedMs, options.RecoveryMsPerMB);
            IIndexCheckpointPolicy indexPolicy = options.EffectiveIndexPolicy switch
            {
                "none" => new NoIndexCheckpointPolicy(),
                "every" => new EveryNthIndexCheckpointPolicy(options.FullCheckpointEvery),
                "rto" => new RtoIndexCheckpointPolicy(recoveryModel, options.RtoMilliseconds, options.RtoSafety),
                _ => throw new InvalidOperationException($"Unknown index policy: {options.EffectiveIndexPolicy}")
            };

            return new CheckpointScheduler<Key, Value>(store, policy, indexPolicy, recoveryModel, OpsDoneSoFar, options.PeriodicCheckpointType,
                options.PeriodicCheckpointTryIncremental, mtbfMs, options.SampleMilliseconds);
        }

        private void ShiftWorkloadMix()
        {
            // Workers read these without synchronization; a mix that is briefly inconsistent across the three fields is harmless.
            readPercent = testLoader.ReadPercent2;
            upsertPercent = testLoader.UpsertPercent2;
            rmwPercent = testLoader.RmwPercent2;
            Console.WriteLine($"Workload mix switched to rumd {string.Join(',', testLoader.Options.RumdPercents2)}");
        }

        private void AppendCheckpointSummary(CheckpointScheduler<Key, Value> scheduler, double opsPerSecond, double recoveryMs)
        {
            var options = testLoader.Options;
            var cycles = scheduler.Cycles;
            var fullCycles = cycles.Where(c => c.Full).ToList();
            var predicted = scheduler.PredictedRecoveryMs.ToList();
            var path = options.ResultPrefix + "_summary.csv";
            if (!File.Exists(path))
                File.WriteAllText(path, "timestamp,policy,checkpoint_type,distribution,rumd,rumd2,shift_sec,threads,run_sec,mtbf_sec,"
                                        + "checkpoints,mean_interval_ms,mean_duration_ms,ops_per_sec,effective_ops_per_sec,"
                                        + "index_policy,full_checkpoints,mean_full_duration_ms,recovery_scan_mb_est,recovery_ms,"
                                        + "rto_ms,max_predicted_recovery_ms,rto_violations" + Environment.NewLine);

            var line = $"{DateTime.Now:s},{options.EffectiveCheckpointPolicy},{options.PeriodicCheckpointType},{testLoader.Distribution},"
                + $"{string.Join(' ', options.RumdPercents)},{string.Join(' ', options.RumdPercents2)},{options.ShiftSeconds},{options.ThreadCount},"
                + $"{options.RunSeconds},{options.MtbfSeconds},{cycles.Count},"
                + FormattableString.Invariant($"{(cycles.Count == 0 ? 0 : cycles.Average(c => c.IntervalMs)):F1},{(cycles.Count == 0 ? 0 : cycles.Average(c => c.DurationMs)):F1},")
                + FormattableString.Invariant($"{opsPerSecond:F0},{scheduler.EffectiveThroughput():F0},")
                + $"{scheduler.IndexPolicyName},"
                + FormattableString.Invariant($"{fullCycles.Count},{(fullCycles.Count == 0 ? 0 : fullCycles.Average(c => c.DurationMs)):F1},")
                + FormattableString.Invariant($"{scheduler.RecoveryScanEstimateBytes / 1e6:F1},{recoveryMs:F1},")
                + FormattableString.Invariant($"{options.RtoMilliseconds},{(predicted.Count == 0 ? 0 : predicted.Max()):F1},{predicted.Count(p => p > options.RtoMilliseconds)}");
            File.AppendAllText(path, line + Environment.NewLine);
            Console.WriteLine($"Checkpoint results written to {options.ResultPrefix}_{{timeline,cycles,summary}}.csv");
        }

        private void SetupYcsbUnsafeContext(int thread_idx)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var session = store.For(functions).NewSession<Functions>();
            var uContext = session.UnsafeContext;
            uContext.BeginUnsafe();

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            Value value = default;

            try
            {
                for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    chunk_idx < InitCount;
                    chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
                {
                    for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                    {
                        if (idx % 256 == 0)
                        {
                            uContext.Refresh();

                            if (idx % 65536 == 0)
                            {
                                uContext.CompletePending(false);
                            }
                        }

                        uContext.Upsert(ref init_keys_[idx], ref value, Empty.Default, 1);
                    }
#if DASHBOARD
                count += (int)kChunkSize;

                //Check if stats collector is requesting for statistics
                if (writeStats[thread_idx])
                {
                    var tstart1 = tstop1;
                    tstop1 = Stopwatch.GetTimestamp();
                    threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                    lastWrittenValue = count;
                    writeStats[thread_idx] = false;
                    statsWritten[thread_idx].Set();
                }
#endif
                }
                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }
            session.Dispose();
        }

        private void SetupYcsbSafeContext(int thread_idx)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var session = store.For(functions).NewSession<Functions>();

            Value value = default;

            for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                chunk_idx < InitCount;
                chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                {
                    if (idx % 256 == 0)
                    {
                        session.Refresh();

                        if (idx % 65536 == 0)
                        {
                            session.CompletePending(false);
                        }
                    }

                    session.Upsert(ref init_keys_[idx], ref value, Empty.Default, 1);
                }
            }

            session.CompletePending(true);
            session.Dispose();
        }

#if DASHBOARD
        int measurementInterval = 2000;
        bool measureLatency;
        bool[] writeStats;
        private EventWaitHandle[] statsWritten;
        double[] threadThroughput;
        double[] threadAverageLatency;
        double[] threadMaximumLatency;
        long[] threadProgress;
        double freq;

        void DoContinuousMeasurements()
        {
            double totalThroughput, totalLatency, maximumLatency;
            double totalProgress;
            int ver = 0;

            using (var client = new WebClient())
            {
                while (!done)
                {
                    ver++;

                    Thread.Sleep(measurementInterval);

                    totalProgress = 0;
                    totalThroughput = 0;
                    totalLatency = 0;
                    maximumLatency = 0;

                    for (int i = 0; i < threadCount; i++)
                    {
                        writeStats[i] = true;
                    }


                    for (int i = 0; i < threadCount; i++)
                    {
                        statsWritten[i].WaitOne();
                        totalThroughput += threadThroughput[i];
                        totalProgress += threadProgress[i];
                        if (measureLatency)
                        {
                            totalLatency += threadAverageLatency[i];
                            if (threadMaximumLatency[i] > maximumLatency)
                            {
                                maximumLatency = threadMaximumLatency[i];
                            }
                        }
                    }

                    if (measureLatency)
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3} \t {4} \t {5}", ver, totalThroughput / (double)1000000, totalLatency / threadCount, maximumLatency, store.Log.TailAddress, totalProgress);
                    }
                    else
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3}", ver, totalThroughput / (double)1000000, store.Log.TailAddress, totalProgress);
                    }
                }
            }
        }
#endif

        #region Load Data

        internal static void CreateKeyVectors(TestLoader testLoader, out Key[] i_keys, out Key[] t_keys)
        {
            InitCount = YcsbConstants.kChunkSize * (testLoader.InitCount / YcsbConstants.kChunkSize);
            TxnCount = YcsbConstants.kChunkSize * (testLoader.TxnCount / YcsbConstants.kChunkSize);

            i_keys = new Key[InitCount];
            t_keys = new Key[TxnCount];
        }

        internal class KeySetter : IKeySetter<Key>
        {
            public void Set(Key[] vector, long idx, long value) => vector[idx].value = value;
        }

#endregion
    }
}
