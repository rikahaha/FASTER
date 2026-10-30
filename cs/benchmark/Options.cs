// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using FASTER.core;
using System.Collections.Generic;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run:" +
                        "\n    0 = YCSB" +
                        "\n    1 = YCSB with SpanByte" +
                        "\n    2 = ConcurrentDictionary")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "NUMA options (Windows only):" +
                        "\n    0 = No sharding across NUMA sockets" +
                        "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "recover", Required = false, Default = false,
             HelpText = "Enable Backup and Restore of FasterKV for fast test startup." +
                        "\n    True = Recover FasterKV if a Checkpoint is available, else populate FasterKV from data and Checkpoint it so it can be Restored in a subsequent run" +
                        "\n    False = Populate FasterKV from data and do not Checkpoint a backup" +
                        "\n    (Checkpoints are stored in directories under " + TestLoader.DataPath + " in directories named by distribution, ycsb vs. synthetic data, and key counts)")]
        public bool BackupAndRestore { get; set; }

        [Option('z', "locking", Required = false, Default = 0,
             HelpText = "Locking Implementation:" +
                        "\n    0 = None (default)" +
                        "\n    1 = LockTable using main HashTable buckets")]
        public int ConcurrencyControlMode { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
         HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('d', "distribution", Required = false, Default = YcsbConstants.UniformDist,
            HelpText = "Distribution of keys in workload")]
        public string DistributionName { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }

        [Option("rumd", Separator = ',', Required = false, Default = new[] {50,50,0,0},
         HelpText = "#,#,#,#: Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] (summing to 100) operations in this run")]
        public IEnumerable<int> RumdPercents { get; set; }

        [Option("synth", Required = false, Default = false,
            HelpText = "Use synthetic data")]
        public bool UseSyntheticData { get; set; }

        [Option("runsec", Required = false, Default = 30,
            HelpText = "Number of seconds to execute experiment")]
        public int RunSeconds { get; set; }

        [Option("sd", Required = false, Default = false,
            HelpText = "Use SmallData in experiment")]
        public bool UseSmallData { get; set; }

        [Option("sm", Required = false, Default = false,
            HelpText = "Use Small Memory log in experiment")]
        public bool UseSmallMemoryLog { get; set; }

        [Option("hashpack", Required = false, Default = 2,
            HelpText = "The hash table packing; divide the number of keys by this to cause hash collisions")]
        public int HashPacking { get; set; }

        [Option("safectx", Required = false, Default = false,
            HelpText = "Use 'safe' context (slower, per-operation epoch control) in experiment")]
        public bool UseSafeContext { get; set; }

        [Option("chkptms", Required = false, Default = 0,
            HelpText = "If > 0, the number of milliseconds between checkpoints in experiment (else checkpointing is not done")]
        public int PeriodicCheckpointMilliseconds { get; set; }

        [Option("chkptsnap", Required = false, Default = false,
            HelpText = "Use Snapshot checkpoint if doing periodic checkpoints (default is FoldOver)")]
        public bool PeriodicCheckpointUseSnapshot { get; set; }

        [Option("chkptincr", Required = false, Default = false,
            HelpText = "Try incremental checkpoint if doing periodic checkpoints")]
        public bool PeriodicCheckpointTryIncremental { get; set; }

        [Option("dumpdist", Required = false, Default = false,
            HelpText = "Dump the distribution of each non-empty bucket in the hash table")]
        public bool DumpDistribution { get; set; }

        [Option("chkptpolicy", Required = false, Default = "",
            HelpText = "Checkpoint scheduling policy:" +
                        "\n    (empty) = fixed if chkptms > 0, else no checkpoints" +
                        "\n    fixed = every chkptms milliseconds" +
                        "\n    youngdaly = Young/Daly optimum sqrt(2 * duration * MTBF)" +
                        "\n    ucb = sliding-window UCB bandit over ucbarms, maximizing effective throughput")]
        public string CheckpointPolicy { get; set; }

        [Option("mtbfsec", Required = false, Default = 60,
            HelpText = "Assumed mean time between failures (seconds), used by the checkpoint objective")]
        public int MtbfSeconds { get; set; }

        [Option("ucbarms", Separator = ',', Required = false, Default = new[] { 250, 500, 1000, 2000, 4000, 8000 },
            HelpText = "Candidate checkpoint intervals (ms) for the ucb policy; also the min/max interval for youngdaly")]
        public IEnumerable<int> UcbArmsMs { get; set; }

        [Option("ucbwindow", Required = false, Default = 30,
            HelpText = "Number of most recent checkpoint cycles the ucb policy remembers")]
        public int UcbWindow { get; set; }

        [Option("ucbc", Required = false, Default = 0.2,
            HelpText = "Exploration coefficient of the ucb policy")]
        public double UcbExploration { get; set; }

        [Option("samplems", Required = false, Default = 100,
            HelpText = "Throughput sampling period (ms) for the checkpoint timeline CSV")]
        public int SampleMilliseconds { get; set; }

        [Option("rumd2", Separator = ',', Required = false, Default = new int[0],
         HelpText = "#,#,#,#: Workload mix to switch to after shiftsec seconds (same format as rumd)")]
        public IEnumerable<int> RumdPercents2 { get; set; }

        [Option("shiftsec", Required = false, Default = 0,
            HelpText = "If > 0 and rumd2 is given, switch the workload mix to rumd2 after this many seconds")]
        public int ShiftSeconds { get; set; }

        [Option("indexpolicy", Required = false, Default = "",
            HelpText = "When checkpoints also checkpoint the index (full checkpoint):" +
                        "\n    (empty) = every if fullevery > 0, else none" +
                        "\n    none = log-only checkpoints" +
                        "\n    every = every fullevery-th checkpoint" +
                        "\n    rto = when predicted recovery time would exceed rtosafety * rtoms")]
        public string IndexPolicy { get; set; }

        [Option("fullevery", Required = false, Default = 0,
            HelpText = "For indexpolicy every: every Nth checkpoint (starting with the first) is a full checkpoint (index + log)")]
        public int FullCheckpointEvery { get; set; }

        [Option("rtoms", Required = false, Default = 1000,
            HelpText = "Recovery time objective (ms): the target of indexpolicy rto, and the threshold for counting violations")]
        public int RtoMilliseconds { get; set; }

        [Option("rtosafety", Required = false, Default = 0.8,
            HelpText = "Fraction of rtoms that indexpolicy rto aims for")]
        public double RtoSafety { get; set; }

        [Option("recfixedms", Required = false, Default = 200.0,
            HelpText = "Recovery model: fixed recovery cost (ms), e.g. loading the index")]
        public double RecoveryFixedMs { get; set; }

        [Option("recmspermb", Required = false, Default = 1.4,
            HelpText = "Recovery model: cost (ms) of replaying each MB of log written since the last index checkpoint")]
        public double RecoveryMsPerMB { get; set; }

        [Option("recoverytest", Required = false, Default = false,
            HelpText = "After the run, discard the store and time recovery from the last checkpoint (requires checkpointing)")]
        public bool RecoveryTest { get; set; }

        [Option("resultprefix", Required = false, Default = "ckpt",
            HelpText = "Prefix of the checkpoint result CSV files")]
        public string ResultPrefix { get; set; }

        internal CheckpointType PeriodicCheckpointType => this.PeriodicCheckpointUseSnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver;

        internal string EffectiveCheckpointPolicy
            => this.CheckpointPolicy != "" ? this.CheckpointPolicy.ToLower() : this.PeriodicCheckpointMilliseconds > 0 ? "fixed" : "none";

        internal bool CheckpointingEnabled => this.EffectiveCheckpointPolicy != "none";

        internal string EffectiveIndexPolicy
            => this.IndexPolicy != "" ? this.IndexPolicy.ToLower() : this.FullCheckpointEvery > 0 ? "every" : "none";

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            return $"d: {DistributionName.ToLower()}; n: {NumaStyle}; rumd: {string.Join(',', RumdPercents)}; t: {ThreadCount}; z: {ConcurrencyControlMode}; i: {IterationCount}; hp: {HashPacking}"
                        + $" sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; sy: {boolStr(this.UseSyntheticData)}; safectx: {boolStr(this.UseSafeContext)};"
                        + $" chkptms: {this.PeriodicCheckpointMilliseconds}; chkpttype: {(this.CheckpointingEnabled ? this.PeriodicCheckpointType.ToString() : "None")};"
                        + $" chkptincr: {boolStr(this.PeriodicCheckpointTryIncremental)}; chkptpolicy: {this.EffectiveCheckpointPolicy}; mtbfsec: {this.MtbfSeconds}; indexpolicy: {this.EffectiveIndexPolicy}; fullevery: {this.FullCheckpointEvery}; rtoms: {this.RtoMilliseconds}"
                        + (this.ShiftSeconds > 0 ? $"; rumd2: {string.Join(',', RumdPercents2)} @ {this.ShiftSeconds}s" : "");
        }
    }
}
