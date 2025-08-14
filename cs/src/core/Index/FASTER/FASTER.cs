// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;   // ✅ 需要 List<>
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal readonly AllocatorBase<Key, Value> hlog;
        internal readonly AllocatorBase<Key, Value> readcache;

        // ====== ✅ 新增/整理：供改造后的 HybridLogCheckpoint 使用 ======
        // 增量扫描的起点（上一次 CP 结束时的 tail）；默认 -1 表示“从未做过 CP”
        internal long lastScannedTailAddress = -1;

        // 一次性强制“下一次 CP 全量”（审计/首轮基线用）
        internal bool ForceFullOnNextCheckpoint;

        // 周期 CP 轻量模式：IN_PROGRESS 跳过内存扫描，降低 CPU 干扰
        internal bool LightweightPeriodicCP = true;

        // 计数器：可选调试/观测
        internal long checkpointCollectedCount; // IN_PROGRESS 内存扫描收集条数
        internal long checkpointFlushedCount;   // 若以后统计写盘数量可用

        // 并行补齐 physical 地址时使用的缓冲；只做按索引读/写，不做 Add/Remove
        internal readonly List<(long logicalAddress, long physicalAddress)> checkpointBuffer = new(1024);
        // ============================================================

        /// <summary>
        /// Compares two keys
        /// </summary>
        internal readonly IFasterEqualityComparer<Key> comparer;

        internal readonly bool UseReadCache;
        private readonly ReadCopyOptions ReadCopyOptions;
        internal readonly int sectorSize;
        private readonly bool WriteDefaultOnDelete;

        /// <summary>
        /// Number of active entries in hash index (does not correspond to total records, due to hash collisions)
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => state[resizeInfo.version].size;

        /// <summary>
        /// Number of overflow buckets in use (64 bytes each)
        /// </summary>
        public long OverflowBucketCount => overflowBucketsAllocator.GetMaxValidAddress();

        /// <summary>
        /// Comparer used by FASTER
        /// </summary>
        public IFasterEqualityComparer<Key> Comparer => comparer;

        /// <summary>
        /// Hybrid log used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value> Log { get; }

        /// <summary>
        /// Read cache used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value> ReadCache { get; }

        ConcurrentDictionary<int, (string, CommitPoint)> _recoveredSessions;
        ConcurrentDictionary<string, int> _recoveredSessionNameMap;
        int maxSessionID;

        internal readonly bool DoTransientLocking;  // uses LockTable
        internal readonly bool DoEphemeralLocking;  // uses RecordInfo
        internal bool IsLocking => DoTransientLocking || DoEphemeralLocking;
        internal readonly bool CheckpointVersionSwitchBarrier;  // version switch barrier
        internal readonly OverflowBucketLockTable<Key, Value> LockTable;

        internal void IncrementNumLockingSessions()
        {
            _hybridLogCheckpoint.info.manualLockingActive = true;
            Interlocked.Increment(ref this.hlog.NumActiveLockingSessions);
        }
        internal void DecrementNumLockingSessions() => Interlocked.Decrement(ref this.hlog.NumActiveLockingSessions);

        internal readonly int ThrottleCheckpointFlushDelayMs = -1;

        /// <summary>
        /// Create FasterKV instance
        /// </summary>
        public FasterKV(FasterKVSettings<Key, Value> fasterKVSettings) :
            this(
                fasterKVSettings.GetIndexSizeCacheLines(), fasterKVSettings.GetLogSettings(),
                fasterKVSettings.GetCheckpointSettings(), fasterKVSettings.GetSerializerSettings(),
                fasterKVSettings.EqualityComparer, fasterKVSettings.GetVariableLengthStructSettings(),
                fasterKVSettings.TryRecoverLatest, fasterKVSettings.ConcurrencyControlMode, null, fasterKVSettings.logger)
        { }

        /// <summary>
        /// Create FasterKV instance
        /// </summary>
        public FasterKV(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null, bool tryRecoverLatest = false, ConcurrencyControlMode concurrencyControlMode = ConcurrencyControlMode.LockTable,
            ILoggerFactory loggerFactory = null, ILogger logger = null, int lockTableSize = Constants.kDefaultLockTableSize)
        {
            this.loggerFactory = loggerFactory;
            this.logger = logger ?? this.loggerFactory?.CreateLogger("FasterKV Constructor");

            if (comparer != null)
                this.comparer = comparer;
            else
            {
                if (typeof(IFasterEqualityComparer<Key>).IsAssignableFrom(typeof(Key)))
                {
                    if (default(Key) is not null)
                        this.comparer = default(Key) as IFasterEqualityComparer<Key>;
                    else if (typeof(Key).GetConstructor(Type.EmptyTypes) != null)
                        this.comparer = Activator.CreateInstance(typeof(Key)) as IFasterEqualityComparer<Key>;
                }
                else
                {
                    this.comparer = FasterEqualityComparer.Get<Key>();
                }
            }

            this.DoTransientLocking = concurrencyControlMode == ConcurrencyControlMode.LockTable;
            this.DoEphemeralLocking = concurrencyControlMode == ConcurrencyControlMode.RecordIsolation;

            if (checkpointSettings is null)
                checkpointSettings = new CheckpointSettings();

            this.CheckpointVersionSwitchBarrier = checkpointSettings.CheckpointVersionSwitchBarrier;
            this.ThrottleCheckpointFlushDelayMs = checkpointSettings.ThrottleCheckpointFlushDelayMs;

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                logger?.LogInformation("CheckpointManager and CheckpointDir specified, ignoring CheckpointDir");

            checkpointManager = checkpointSettings.CheckpointManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        new DirectoryInfo(checkpointSettings.CheckpointDir ?? ".").FullName), removeOutdated: checkpointSettings.RemoveOutdated);

            if (checkpointSettings.CheckpointManager is null)
                disposeCheckpointManager = true;

            UseReadCache = logSettings.ReadCacheSettings is not null;

            this.ReadCopyOptions = logSettings.ReadCopyOptions;
            if (this.ReadCopyOptions.CopyTo == ReadCopyTo.Inherit)
                this.ReadCopyOptions.CopyTo = UseReadCache ? ReadCopyTo.ReadCache : ReadCopyTo.None;
            else if (this.ReadCopyOptions.CopyTo == ReadCopyTo.ReadCache && !UseReadCache)
                this.ReadCopyOptions.CopyTo = ReadCopyTo.None;

            if (this.ReadCopyOptions.CopyFrom == ReadCopyFrom.Inherit)
                this.ReadCopyOptions.CopyFrom = ReadCopyFrom.Device;

            UpdateVarLen(ref variableLengthStructSettings);
            IVariableLengthStruct<Key> keyLen = null;

            if ((!Utility.IsBlittable<Key>() && variableLengthStructSettings?.keyLength is null) ||
                (!Utility.IsBlittable<Value>() && variableLengthStructSettings?.valueLength is null))
            {
                WriteDefaultOnDelete = true;

                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer, null, epoch, logger: logger ?? loggerFactory?.CreateLogger("GenericAllocator HybridLog"));
                Log = new LogAccessor<Key, Value>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new GenericAllocator<Key, Value>(
                        new LogSettings
                        {
                            LogDevice = new NullDevice(),
                            ObjectLogDevice = new NullDevice(),
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, serializerSettings, this.comparer, ReadCacheEvict, epoch, logger: logger ?? loggerFactory?.CreateLogger("GenericAllocator ReadCache"));
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value>(this, readcache);
                }
            }
            else if (variableLengthStructSettings != null)
            {
                keyLen = variableLengthStructSettings.keyLength;
                hlog = new VariableLengthBlittableAllocator<Key, Value>(logSettings, variableLengthStructSettings,
                    this.comparer, null, epoch, logger: logger ?? loggerFactory?.CreateLogger("VariableLengthAllocator HybridLog"));
                Log = new LogAccessor<Key, Value>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new VariableLengthBlittableAllocator<Key, Value>(
                        new LogSettings
                        {
                            LogDevice = new NullDevice(),
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, variableLengthStructSettings, this.comparer, ReadCacheEvict, epoch, logger: logger ?? loggerFactory?.CreateLogger("VariableLengthAllocator ReadCache"));
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value>(this, readcache);
                }
            }
            else
            {
                hlog = new BlittableAllocator<Key, Value>(logSettings, this.comparer, null, epoch, logger: logger ?? loggerFactory?.CreateLogger("BlittableAllocator HybridLog"));
                Log = new LogAccessor<Key, Value>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new BlittableAllocator<Key, Value>(
                        new LogSettings
                        {
                            LogDevice = new NullDevice(),
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, this.comparer, ReadCacheEvict, epoch, logger: logger ?? loggerFactory?.CreateLogger("VariableLengthAllocator ReadCache"));
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value>(this, readcache);
                }
            }

            hlog.Initialize();

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(size, sectorSize);

            this.LockTable = new OverflowBucketLockTable<Key, Value>(concurrencyControlMode == ConcurrencyControlMode.LockTable ? this : null);

            systemState = SystemState.Make(Phase.REST, 1);

            if (tryRecoverLatest)
            {
                try
                {
                    Recover();
                }
                catch { }
            }
        }

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(Key key) => this.comparer.GetHashCode64(ref key);

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(ref Key key) => this.comparer.GetHashCode64(ref key);

        // ===== 下面保持你原有实现，不改动 =====

        public bool TryInitiateFullCheckpoint(out Guid token, CheckpointType checkpointType, long targetVersion = -1)
        {
            ISynchronizationTask backend;
            if (checkpointType == CheckpointType.FoldOver)
                backend = new FoldOverCheckpointTask();
            else if (checkpointType == CheckpointType.Snapshot)
                backend = new SnapshotCheckpointTask();
            else
                throw new FasterException("Unsupported full checkpoint type");

            var result = StartStateMachine(new FullCheckpointStateMachine(backend, targetVersion));
            if (result)
                token = _hybridLogCheckpointToken;
            else
                token = default;
            return result;
        }

        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType,
            CancellationToken cancellationToken = default, long targetVersion = -1)
        {
            var success = TryInitiateFullCheckpoint(out Guid token, checkpointType, targetVersion);
            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);
            return (success, token);
        }

        public bool TryInitiateIndexCheckpoint(out Guid token)
        {
            var result = StartStateMachine(new IndexSnapshotStateMachine());
            token = _indexCheckpointToken;
            return result;
        }

        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var success = TryInitiateIndexCheckpoint(out Guid token);
            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);
            return (success, token);
        }

        public bool TryInitiateHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false,
            long targetVersion = -1)
        {
            ISynchronizationTask backend;
            if (checkpointType == CheckpointType.FoldOver)
                backend = new FoldOverCheckpointTask();
            else if (checkpointType == CheckpointType.Snapshot)
            {
                if (tryIncremental && _lastSnapshotCheckpoint.info.guid != default && _lastSnapshotCheckpoint.info.finalLogicalAddress > hlog.FlushedUntilAddress && (hlog is not GenericAllocator<Key, Value>))
                    backend = new IncrementalSnapshotCheckpointTask();
                else
                    backend = new SnapshotCheckpointTask();
            }
            else
                throw new FasterException("Unsupported checkpoint type");

            var result = StartStateMachine(new HybridLogCheckpointStateMachine(backend, targetVersion));
            token = _hybridLogCheckpointToken;
            return result;
        }

        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType,
            bool tryIncremental = false, CancellationToken cancellationToken = default, long targetVersion = -1)
        {
            var success = TryInitiateHybridLogCheckpoint(out Guid token, checkpointType, tryIncremental, targetVersion);
            if (success)
                await CompleteCheckpointAsync(cancellationToken).ConfigureAwait(false);
            return (success, token);
        }

        public long Recover(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1)
        {
            FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
            return InternalRecover(recoveredIcInfo, recoveredHlcInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        public ValueTask<long> RecoverAsync(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1,
            CancellationToken cancellationToken = default)
        {
            FindRecoveryInfo(recoverTo, out var recoveredHlcInfo, out var recoveredIcInfo);
            return InternalRecoverAsync(recoveredIcInfo, recoveredHlcInfo, numPagesToPreload, undoNextVersion, recoverTo, cancellationToken);
        }

        public long Recover(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true)
            => InternalRecover(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoNextVersion, -1);

        public ValueTask<long> RecoverAsync(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default)
            => InternalRecoverAsync(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoNextVersion, -1, cancellationToken);

        public long Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true)
            => InternalRecover(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoNextVersion, -1);

        public IEnumerable<(int, string, CommitPoint)> RecoverableSessions
        {
            get
            {
                if (_recoveredSessions != null)
                    foreach (var kvp in _recoveredSessions)
                        yield return (kvp.Key, kvp.Value.Item1, kvp.Value.Item2);
            }
        }

        public void DisposeRecoverableSession(int sessionID)
        {
            if (_recoveredSessions != null && _recoveredSessions.TryRemove(sessionID, out var entry))
            {
                if (entry.Item1 != null)
                    _recoveredSessionNameMap.TryRemove(entry.Item1, out _);
            }
        }

        public void DisposeRecoverableSessions()
        {
            _recoveredSessions = null;
            _recoveredSessionNameMap = null;
        }

        public ValueTask<long> RecoverAsync(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default)
            => InternalRecoverAsync(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoNextVersion, -1, cancellationToken);

        public async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            if (epoch.ThisInstanceProtected())
                throw new FasterException("Cannot use CompleteCheckpointAsync when using non-async sessions");

            token.ThrowIfCancellationRequested();

            while (true)
            {
                var systemState = this.systemState;
                if (systemState.Phase == Phase.REST || systemState.Phase == Phase.PREPARE_GROW ||
                    systemState.Phase == Phase.IN_PROGRESS_GROW)
                    return;

                List<ValueTask> valueTasks = new();

                try
                {
                    epoch.Resume();
                    ThreadStateMachineStep<Empty, Empty, Empty, NullFasterSession>(null, NullFasterSession.Instance, valueTasks, token);
                }
                catch (Exception)
                {
                    this._indexCheckpoint.Reset();
                    this._hybridLogCheckpoint.Dispose();
                    throw;
                }
                finally
                {
                    epoch.Suspend();
                }

                if (valueTasks.Count == 0)
                    continue;

                foreach (var task in valueTasks)
                    if (!task.IsCompleted)
                        await task.ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context>(fasterSession.Ctx.ReadCopyOptions);
            OperationStatus internalStatus;
            var keyHash = comparer.GetHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, Constants.kInvalidAddress, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context context,
                FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context>(fasterSession.Ctx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;
            var keyHash = readOptions.KeyHash ?? comparer.GetHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, readOptions.StartAddress, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<Input, Output, Context, FasterSession>(ref Input input, ref Output output, ref ReadOptions readOptions, Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context>(fasterSession.Ctx.ReadCopyOptions, ref readOptions, noKey: true);
            Key key = default;
            OperationStatus internalStatus;
            do
                internalStatus = InternalRead(ref key, keyHash: 0L, ref input, ref output, readOptions.StartAddress, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<Input, Output, Context, FasterSession>(ref Key key, long keyHash, ref Input input, ref Value value, ref Output output, Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<Input, Output, Context, FasterSession>(ref Key key, long keyHash, ref Input input, ref Value value, ref Output output, out RecordMetadata recordMetadata,
                                                                            Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<Input, Output, Context, FasterSession>(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata,
                                                                          Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRMW(ref key, keyHash, ref input, ref output, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<Input, Output, Context, FasterSession>(ref Key key, long keyHash, Context context, FasterSession fasterSession, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalDelete(ref key, keyHash, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            var status = HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus);

            Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
            fasterSession.Ctx.serialNum = serialNo;
            return status;
        }

        public bool GrowIndex()
        {
            if (epoch.ThisInstanceProtected())
                throw new FasterException("Cannot use GrowIndex when using non-async sessions");

            if (!StartStateMachine(new IndexResizeStateMachine())) return false;

            epoch.Resume();

            try
            {
                while (true)
                {
                    SystemState _systemState = SystemState.Copy(ref systemState);
                    if (_systemState.Phase == Phase.PREPARE_GROW)
                        ThreadStateMachineStep<Empty, Empty, Empty, NullFasterSession>(null, NullFasterSession.Instance, default);
                    else if (_systemState.Phase == Phase.IN_PROGRESS_GROW)
                        SplitBuckets(0);
                    else if (_systemState.Phase == Phase.REST)
                        break;
                    epoch.ProtectAndDrain();
                    Thread.Yield();
                }
            }
            finally
            {
                epoch.Suspend();
            }
            return true;
        }

        public void Dispose()
        {
            Free();
            hlog.Dispose();
            readcache?.Dispose();
            LockTable.Dispose();
            _lastSnapshotCheckpoint.Dispose();
            if (disposeCheckpointManager)
                checkpointManager?.Dispose();
        }

        private static void UpdateVarLen(ref VariableLengthStructSettings<Key, Value> variableLengthStructSettings)
        {
            if (typeof(Key) == typeof(SpanByte))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<SpanByte, Value>() as VariableLengthStructSettings<Key, Value>;

                if (variableLengthStructSettings.keyLength == null)
                    (variableLengthStructSettings as VariableLengthStructSettings<SpanByte, Value>).keyLength = new SpanByteVarLenStruct();
            }
            else if (typeof(Key).IsGenericType && (typeof(Key).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Key).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.keyLength == null)
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Key).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.keyLength = o as IVariableLengthStruct<Key>;
                }
            }
            else if (typeof(Key).IsGenericType && (typeof(Key).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Key).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.keyLength == null)
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Key).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.keyLength = o as IVariableLengthStruct<Key>;
                }
            }

            if (typeof(Value) == typeof(SpanByte))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, SpanByte>() as VariableLengthStructSettings<Key, Value>;

                if (variableLengthStructSettings.valueLength == null)
                    (variableLengthStructSettings as VariableLengthStructSettings<Key, SpanByte>).valueLength = new SpanByteVarLenStruct();
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.valueLength == null)
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.valueLength = o as IVariableLengthStruct<Value>;
                }
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.valueLength == null)
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.valueLength = o as IVariableLengthStruct<Value>;
                }
            }
        }

        private unsafe long GetEntryCount()
        {
            var version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_entry_count = 0;
            long beginAddress = hlog.BeginAddress;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                        if (b.bucket_entries[bucket_entry] >= beginAddress)
                            ++total_entry_count;
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask) == 0) break;
                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask);
                }
            }
            return total_entry_count;
        }

        private unsafe string DumpDistributionInternal(int version)
        {
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_record_count = 0;
            long beginAddress = hlog.BeginAddress;
            Dictionary<int, long> histogram = new();

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                List<int> tags = new();
                int cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        var x = default(HashBucketEntry);
                        x.word = b.bucket_entries[bucket_entry];
                        if (((!x.ReadCache) && (x.Address >= beginAddress)) || (x.ReadCache && (x.AbsoluteAddress >= readcache.HeadAddress)))
                        {
                            if (tags.Contains(x.Tag) && !x.Tentative)
                                throw new FasterException("Duplicate tag found in index");
                            tags.Add(x.Tag);
                            ++cnt;
                            ++total_record_count;
                        }
                    }
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask) == 0) break;
                    b = *(HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & Constants.kAddressMask);
                }

                if (!histogram.ContainsKey(cnt)) histogram[cnt] = 0;
                histogram[cnt]++;
            }

            var distribution =
                $"Number of hash buckets: {table_size_}\n" +
                $"Number of overflow buckets: {OverflowBucketCount}\n" +
                $"Size of each bucket: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry)} bytes\n" +
                $"Total distinct hash-table entry count: {{{total_record_count}}}\n" +
                $"Average #entries per hash bucket: {{{total_record_count / (double)table_size_:0.00}}}\n" +
                $"Histogram of #entries per bucket:\n";

            foreach (var kvp in histogram.OrderBy(e => e.Key))
                distribution += $"  {kvp.Key} : {kvp.Value}\n";

            return distribution;
        }

        public string DumpDistribution() => DumpDistributionInternal(resizeInfo.version);
    }
}
