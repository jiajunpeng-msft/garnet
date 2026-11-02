// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    /// <summary>
    /// Initialize can restart a log at an address below its previous tail, as a replica does when a full sync adopts its primary's
    /// AOF addresses. Uncommitted scans must then stop at the restarted log's tail rather than the discarded log's.
    /// </summary>
    [TestFixture]
    internal class LogInitializeLowerAddressTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public void UncommittedScanStopsAtTailAfterInitializeToLowerAddress()
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "initialize-lower.log"), deleteOnClose: true);
            log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                MemorySizeBits = 20,
                PageSizeBits = 14,
                LogCommitManager = manager,
                TryRecoverLatest = false
            });

            var filler = new byte[1000];
            for (var i = 0; i < 100; i++)
                _ = log.Enqueue(filler);
            while (log.RefreshSafeTailAddress() < log.TailAddress)
                _ = Thread.Yield();
            log.Commit(spinWait: true);
            Assert.That(log.TailAddress, Is.GreaterThan(4 * (1L << 14)), "Precondition: the discarded log spans several pages");

            var beginAddress = log.BeginAddress;
            log.Initialize(beginAddress, beginAddress);
            Assert.That(log.TailAddress, Is.EqualTo(beginAddress));

            byte[] entry = [1, 2, 3, 4];
            _ = log.Enqueue(entry);
            while (log.RefreshSafeTailAddress() < log.TailAddress)
                _ = Thread.Yield();

            using var iterator = log.Scan(beginAddress, long.MaxValue, scanUncommitted: true);
            Assert.That(iterator.GetNext(out var result, out _, out _), Is.True);
            Assert.That(result, Is.EqualTo(entry));
            Assert.That(iterator.GetNext(out _, out _, out _), Is.False, "The scan must end at the restarted log's tail");
            Assert.That(log.SafeTailAddress, Is.EqualTo(log.TailAddress));
        }
    }
}