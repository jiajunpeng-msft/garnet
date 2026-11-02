// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// The resizer must not move HeadAddress into a record that a writer has allocated but not yet written.
    /// </summary>
    /// <remarks>
    /// A page-turning writer publishes TailAddress before it writes its record, so the record's bytes are still zero while
    /// the resizer runs. The resizer walks records up to <c>TailAddress - MinEvictionHeadAddressLag</c>; for a record longer
    /// than that lag, the limit falls inside the unwritten record and the walk steps over its zero bytes to that limit.
    /// HeadAddress (and ReadOnlyAddress) then land mid-record, and once the record is written every later walk from HeadAddress,
    /// such as the read-only flush, decodes value bytes as record headers.
    /// </remarks>
    [TestFixture]
    internal class LogSizeTrackerUnwrittenRecordTests : TestBase
    {
        const int PageSize = 1 << 13;                               // 8 KB
        const long LogMemorySize = PageSize << 4;
        const long TargetSize = PageSize * LogSizeTracker.MinTargetPageCount;

        // Longer than MinEvictionHeadAddressLag, so the resizer's scan limit falls inside the record.
        const int UnwrittenRecordSize = LogSizeTracker.MinEvictionHeadAddressLag + 1904;

        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(60);

        TsavoriteKV<ObjStoreFunctions, ObjAllocator> store;
        LogSizeTracker<ObjStoreFunctions, ObjAllocator> tracker;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "UnwrittenRecord.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "UnwrittenRecord.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = LogMemorySize,
                PageSize = PageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SizedHeapObject.Serializer(), DefaultRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            tracker = new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store.Log, TargetSize, TargetSize / 8, TargetSize / 16, logger: null);
            store.Log.SetLogSizeTracker(tracker);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            tracker = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void ResizerStopsAtTheStartOfAnUnwrittenRecord()
        {
            // Fill most of the first page with records that carry heap objects, so the resizer walks records rather than pages.
            using (var session = store.NewSession<TestObjectKey, Empty, Empty, Empty, SizedHeapObjectFunctions>(new SizedHeapObjectFunctions()))
            {
                var bContext = session.BasicContext;
                var key = 0;
                while (store.hlogBase.GetTailAddress() < PageSize - (UnwrittenRecordSize / 2))
                    _ = bContext.Upsert(new TestObjectKey { key = key++ }, new SizedHeapObject(8));
            }

            // Allocate a record that does not fit on the first page, as a writer does before it writes the record:
            // TailAddress now covers the record on the second page, but its bytes are still zero.
            long unwrittenRecordAddress;
            store.epoch.Resume();
            try
            {
                Assert.That(store.hlogBase.TryAllocateRetryNow(UnwrittenRecordSize, out unwrittenRecordAddress), Is.True);
            }
            finally
            {
                store.epoch.Suspend();
            }
            Assert.That(unwrittenRecordAddress, Is.EqualTo(store.hlogBase.GetFirstValidLogicalAddressOnPage(1)),
                "Precondition: the unwritten record is the first record of the tail page");

            // Hold the overage outside any record, so the resizer cannot satisfy it before reaching its scan limit.
            tracker.IncrementSize(TargetSize * 4);
            Assert.That(tracker.IsOverBudget, Is.True, $"Test did not exceed the tracker budget. Tracker: {tracker}");

            var startingHeadAddress = store.hlogBase.HeadAddress;
            tracker.Start(CancellationToken.None);
            var deadline = DateTime.UtcNow + CompletionTimeout;
            while (store.hlogBase.HeadAddress == startingHeadAddress && DateTime.UtcNow < deadline)
                _ = Thread.Yield();
            var stop = Task.Run(() => tracker.Stop(wait: true));
            Assert.That(stop.Wait(CompletionTimeout), Is.True, $"The resizer did not stop within {CompletionTimeout}. Tracker: {tracker}");
            stop.GetAwaiter().GetResult();

            Assert.That(store.hlogBase.HeadAddress, Is.GreaterThan(startingHeadAddress), $"Precondition: the resizer evicted. Tracker: {tracker}");
            Assert.That(store.hlogBase.HeadAddress, Is.LessThanOrEqualTo(unwrittenRecordAddress),
                $"HeadAddress {store.hlogBase.HeadAddress} must not fall inside the unwritten record at {unwrittenRecordAddress}");
        }
    }
}