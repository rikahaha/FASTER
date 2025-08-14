using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;
// FASTER.benchmark


namespace FASTER.benchmark
{
    /// <summary>
    /// 统一的一致性检查：
    /// 1) 记录 checkpoint 前的 ground-truth（全量或采样）
    /// 2) 调用 FoldOver/你的CPR实现进行 checkpoint
    /// 3) 模拟 crash（Dispose）
    /// 4) Recover
    /// 5) 扫描并对比
    /// </summary>
    public static class ConsistencyAuditor
    {
        public enum Mode { Off = 0, Sample = 1, Full = 2 }

        public sealed class Options
        {
            public Mode CheckMode = Mode.Off;
            public int SampleCount = 100_000;     // 采样检查的 key 数
            public string DataPath = "benchmark-data"; // 日志目录（需与主程序一致）
            public bool VerboseDiff = false;      // 打印差异明细
        }

        /// <summary>
        /// 供 benchmark 调用的一站式入口。
        /// 注意：调用前应确保停止产生新请求，或在调用内主动 quiesce。
        /// </summary>
        public static async Task RunAndAuditAsync(
            FasterKV<long, long> store,
            IDevice logDevice,
            Options opt,
            Func<IEnumerable<long>>? keySampler = null,
            CheckpointType checkpointType = CheckpointType.FoldOver)
        {
            if (opt.CheckMode == Mode.Off) return;

            // 1) Ground truth：在 checkpoint 前获取
            //   - Full: 扫描 log 到当前 Tail，得到最新 <k,v>
            //   - Sample: 若提供 sampler，就按 sampler 读取；否则从扫描中抽样
            var groundTruth = new Dictionary<long, long>();

            if (opt.CheckMode == Mode.Full)
            {
                BuildGroundTruthByScan(store, groundTruth);
            }
            else
            {
                if (keySampler != null)
                    BuildGroundTruthByRead(store, keySampler(), opt.SampleCount, groundTruth);
                else
                    BuildGroundTruthByScanSample(store, opt.SampleCount, groundTruth);
            }

            // 2) 触发 checkpoint（这里会走到你改过的 CPR 流程）
            await store.TakeFullCheckpointAsync(checkpointType).ConfigureAwait(false);

            // （可选）为了让后续恢复干净，你也可以移动 begin address
            // store.Log.ShiftBeginAddress(store.Log.TailAddress);

            // 3) 模拟 crash：Dispose 现有实例
            store.Dispose();
            logDevice.Dispose();

            // 4) 恢复
            var logPath = Path.Combine(opt.DataPath, "hlog.log");
            var log2 = Devices.CreateLogDevice(logPath, preallocateFile: false);

            var store2 = new FasterKV<long, long>(
                size: 1L << 20,
                new LogSettings { LogDevice = log2 });

            store2.Recover();   // <-- 从 checkpoint 恢复

            // 5) 对比
            var recovered = new Dictionary<long, long>();

            if (opt.CheckMode == Mode.Full)
            {
                BuildGroundTruthByScan(store2, recovered);
            }
            else
            {
                // 用 groundTruth 的 key 子集来验证
                BuildGroundTruthByRead(store2, groundTruth.Keys, opt.SampleCount, recovered);
            }

            // 6) diff
            int mismatch = 0, missing = 0;
            foreach (var (k, v) in groundTruth)
            {
                if (!recovered.TryGetValue(k, out var rv))
                {
                    missing++;
                    if (opt.VerboseDiff) Console.WriteLine($"[MISS] key={k}, expect={v}");
                }
                else if (rv != v)
                {
                    mismatch++;
                    if (opt.VerboseDiff) Console.WriteLine($"[DIFF] key={k}, expect={v}, got={rv}");
                }
            }

            if (missing == 0 && mismatch == 0)
                Console.WriteLine("✅ Consistency OK (no missing/mismatch)");
            else
                Console.WriteLine($"❌ Consistency FAIL (missing={missing}, mismatch={mismatch})");

            // 7) 清理或把恢复后的实例交回调用方（本工具里直接释放）
            store2.Dispose();
            log2.Dispose();
        }

        private static void BuildGroundTruthByScan(FasterKV<long, long> store, Dictionary<long, long> dest)
        {
            using var it = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
            while (it.GetNext(out var info))
            {
                if (!info.Invalid)
                {
                    long k = it.GetKey();
                    long v = it.GetValue();
                    dest[k] = v; // 以最后出现为准
                }
            }
        }

        private static void BuildGroundTruthByScanSample(
            FasterKV<long, long> store, int sampleCount, Dictionary<long, long> dest)
        {
            // 简单的 1/N 抽样
            int picked = 0, step = Math.Max(1, sampleCount / 10); // 粗糙控量
            long i = 0;
            using var it = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
            while (it.GetNext(out var info))
            {
                if (!info.Invalid)
                {
                    if ((i++ % step) == 0)
                    {
                        long k = it.GetKey();
                        long v = it.GetValue();
                        dest[k] = v;
                        if (++picked >= sampleCount) break;
                    }
                }
            }
        }

        private static void BuildGroundTruthByRead(
            FasterKV<long, long> store,
            IEnumerable<long> keys,
            int limit,
            Dictionary<long, long> dest)
        {
            int cnt = 0;
            using var session = store.NewSession(new SimpleFunctions<long, long>());
            foreach (var k in keys)
            {
                var key = k;
                var input = 0L;
                var output = 0L;
                var status = session.Read(ref key, ref input, ref output);
                if (status.Found)
                    dest[k] = output;
                if (++cnt >= limit) break;
            }
        }
    }
}
