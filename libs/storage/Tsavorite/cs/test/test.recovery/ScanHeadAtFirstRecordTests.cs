// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// When HeadAddress is the first record address of a page, ReadOnlyAddress (and so FlushedUntilAddress) can stop at that same address,
    /// leaving the rest of the page unflushed. A scan must then serve the page from memory and must not load and parse it from the device.
    /// </summary>
    [TestFixture]
    internal class ScanHeadAtFirstRecordTests : TestBase
    {
        const long PageSize = 1L << 15;

        TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        IDevice log, objlog;

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void ScanServesPageFromMemoryWhenHeadIsItsFirstRecord([Values] bool staleSegmentBytes)
        {
            var logPath = Path.Join(MethodTestDir, "scan-head.log");
            if (staleSegmentBytes)
            {
                // Bytes left in a reused log segment by an earlier incarnation of the log.
                _ = Directory.CreateDirectory(MethodTestDir);
                var stale = new byte[1 << 20];
                Array.Fill(stale, (byte)0x22);
                File.WriteAllBytes(logPath + ".0", stale);
            }

            log = Devices.CreateLogDevice(logPath, deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan-head.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = 1L << 18,
                PageSize = PageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            var count = 0;
            while (store.Log.TailAddress < 3 * PageSize + PageSize / 2)
            {
                ClassicAssert.IsTrue(context.Upsert(new TestObjectKey { key = count }, new TestObjectValue { value = count }).IsCompletedSuccessfully);
                count++;
            }

            // Evict every page below the tail page. HeadAddress is the tail page's first record, so the tail page is only flushed through its header.
            var firstRecordOnTailPage = (store.Log.TailAddress & ~(PageSize - 1)) + PageHeader.Size;
            store.Log.ShiftHeadAddress(firstRecordOnTailPage, wait: true);
            Assert.That(store.Log.HeadAddress, Is.EqualTo(firstRecordOnTailPage));
            Assert.That(store.Log.FlushedUntilAddress, Is.EqualTo(firstRecordOnTailPage), "Precondition: the tail page's records are not on the device");

            var scanned = 0;
            using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                while (iterator.GetNext())
                {
                    Assert.That(iterator.Key.AsRef<TestObjectKey>().key, Is.EqualTo(scanned));
                    Assert.That(((TestObjectValue)iterator.ValueObject).value, Is.EqualTo(scanned));
                    scanned++;
                }
            }

            Assert.That(scanned, Is.EqualTo(count));
        }
    }
}