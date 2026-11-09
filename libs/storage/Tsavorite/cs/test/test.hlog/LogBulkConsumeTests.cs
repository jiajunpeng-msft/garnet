// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    [NonParallelizable]
    internal class LogBulkConsumeTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup()
        {
            BaseSetup(false);
            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "bulk.log"),
                deleteOnClose: true, deviceType: DeviceType.FileStream);
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                LogCommitManager = manager,
                LogChecksum = LogChecksumType.None,
                FastCommitMode = true,
                AutoCommit = false,
                TryRecoverLatest = false,
                PageSizeBits = 15,
                MemorySizeBits = 17,
                SegmentSizeBits = 22,
                MutableFraction = 0.5
            });
        }

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test, CancelAfter(30_000)]
        public async Task BulkConsumptionCommitBoundaries([Values] bool startsWithCommit,
            [Values] bool stopAtCommitRecord, CancellationToken cancellationToken)
        {
            var records = new List<byte>();
            if (!startsWithCommit)
                AppendData(records, 1);
            _ = AppendCommit(records, 1);
            _ = AppendCommit(records, 2);
            AppendData(records, 2);
            AppendData(records, 3);
            _ = log.UnsafeEnqueueRaw(records.ToArray(), noCommit: true);

            var untilAddress = log.TailAddress;
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var consumer = new ReplayConsumer(log, untilAddress, cancellation);
            using var iterator = log.ScanSingle(log.BeginAddress, untilAddress, scanUncommitted: true);
            var scan = Task.Run(() => stopAtCommitRecord
                ? iterator.BulkConsumeAllAsync(consumer, true, token: cancellation.Token)
                : iterator.BulkConsumeAllAsync(consumer, token: cancellation.Token));
            await FinishScanAsync(scan, cancellation.Token).ConfigureAwait(false);

            Assert.That(consumer.Finished, Is.True);
            Assert.That(consumer.Data, Is.EqualTo(startsWithCommit ? new[] { 2, 3 } : new[] { 1, 2, 3 }));
            Assert.That(consumer.MidChunkCommit, Is.EqualTo(!stopAtCommitRecord));
            Assert.That(consumer.Chunks.Count, Is.EqualTo(stopAtCommitRecord ? 3 : 1));
            if (stopAtCommitRecord)
            {
                Assert.That(consumer.Chunks[0].Records, Is.EqualTo(startsWithCommit ? 1 : 2));
                Assert.That(consumer.Chunks[0].EndsWithCommit, Is.True);
                Assert.That(consumer.Chunks[1].Records, Is.EqualTo(1));
                Assert.That(consumer.Chunks[1].EndsWithCommit, Is.True);
            }
        }

        [Test, CancelAfter(30_000)]
        public async Task CommitBoundaryPersistsBeforeLaterReplayAndResumesFromDisk(CancellationToken cancellationToken)
        {
            var records = new List<byte>();
            AppendData(records, 1);
            var commitAddress = AppendCommit(records, 1);
            AppendData(records, 2);
            AppendData(records, 3);
            _ = log.UnsafeEnqueueRaw(records.ToArray(), noCommit: true);

            var untilAddress = log.TailAddress;
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var consumer = new ReplayConsumer(log, untilAddress, cancellation, pauseAfterFirst: true);
            using var iterator = log.ScanSingle(log.BeginAddress, untilAddress, scanUncommitted: true);
            var scan = Task.Run(() => iterator.BulkConsumeAllAsync(consumer, true, token: cancellation.Token));
            try
            {
                var first = await Task.WhenAny(consumer.Paused, scan).WaitAsync(cancellationToken).ConfigureAwait(false);
                if (first == scan && !consumer.Paused.IsCompleted)
                    await scan.ConfigureAwait(false);

                Assert.That(consumer.MidChunkCommit, Is.False);
                Assert.That(consumer.Data, Is.EqualTo(new[] { 1 }));
                Assert.That(consumer.Chunks[0].IsProtected, Is.True);

                await log.WaitForCommitAsync(commitAddress, commitNum: 1, token: cancellationToken).ConfigureAwait(false);
                Assert.That(log.CommittedUntilAddress, Is.GreaterThanOrEqualTo(commitAddress));
                Assert.That(consumer.Data, Is.EqualTo(new[] { 1 }), "Persistence must not wait for later replay.");

                var padding = new byte[4096];
                for (var i = 0; i < 128; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _ = log.Enqueue(padding);
                }
                Assert.That(log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(1L << 15));

                consumer.Release();
                await FinishScanAsync(scan, cancellation.Token).ConfigureAwait(false);
                Assert.That(consumer.Finished, Is.True);
                Assert.That(consumer.Data, Is.EqualTo(new[] { 1, 2, 3 }));
                Assert.That(consumer.Chunks[1].IsProtected, Is.False, "Evicted input must be reloaded into the disk frame.");
            }
            finally
            {
                cancellation.Cancel();
                consumer.Release();
                await FinishScanAsync(scan, cancellation.Token).ConfigureAwait(false);
            }
        }

        static void AppendData(List<byte> records, int value)
        {
            records.AddRange(BitConverter.GetBytes(sizeof(int)));
            records.AddRange(BitConverter.GetBytes(value));
        }

        long AppendCommit(List<byte> records, long commitNum)
        {
            var info = new TsavoriteLogRecoveryInfo
            {
                BeginAddress = log.BeginAddress,
                CommitNum = commitNum
            };
            info.UntilAddress = log.BeginAddress + records.Count + log.HeaderSize + TsavoriteLog.UnsafeAlign(info.SerializedSize());
            var payload = info.ToByteArray();
            records.AddRange(BitConverter.GetBytes(-payload.Length));
            records.AddRange(payload);
            return info.UntilAddress;
        }

        static async Task FinishScanAsync(Task scan, CancellationToken cancellationToken)
        {
            try
            {
                await scan.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }

        sealed class ReplayConsumer(TsavoriteLog log, long untilAddress,
            CancellationTokenSource cancellation, bool pauseAfterFirst = false) : IBulkLogEntryConsumer, IDisposable
        {
            readonly TaskCompletionSource<bool> paused = new(TaskCreationOptions.RunContinuationsAsynchronously);
            readonly ManualResetEventSlim resume = new();
            int hasPaused;

            internal List<int> Data { get; } = [];
            internal List<(int Records, bool EndsWithCommit, bool IsProtected)> Chunks { get; } = [];
            internal Task Paused => paused.Task;
            internal bool MidChunkCommit { get; private set; }
            internal bool Finished { get; private set; }

            public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
            {
                var count = 0;
                var lastWasCommit = false;
                for (var ptr = record; ptr < record + recordLength;)
                {
                    var length = log.UnsafeGetLength(ptr);
                    var size = log.HeaderSize + TsavoriteLog.UnsafeAlign(Math.Abs(length));
                    lastWasCommit = length < 0;
                    if (lastWasCommit)
                    {
                        MidChunkCommit |= ptr + size < record + recordLength;
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new ReadOnlySpan<byte>(ptr + log.HeaderSize, -length));
                        log.UnsafeCommitMetadataOnly(info, isProtected);
                    }
                    else if (length > 0)
                    {
                        Data.Add(*(int*)(ptr + log.HeaderSize));
                    }
                    ptr += size;
                    count++;
                }
                Chunks.Add((count, lastWasCommit, isProtected));
                if (nextAddress == untilAddress)
                {
                    Finished = true;
                    cancellation.Cancel();
                }
            }

            public void Throttle()
            {
                if (pauseAfterFirst && Chunks.Count == 1 && Interlocked.Exchange(ref hasPaused, 1) == 0)
                {
                    _ = paused.TrySetResult(true);
                    resume.Wait(cancellation.Token);
                }
            }

            internal void Release() => resume.Set();

            public void Dispose() => resume.Dispose();
        }
    }
}