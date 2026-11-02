// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// DEL must count live keys even when deletion appends a tombstone instead of updating in place.
    /// A zero reply in these tests is a count error, not evidence that deletion lost data.
    /// </summary>
    [TestFixture]
    public class RespDeleteCountTests : TestBase
    {
        public enum LogState
        {
            Mutable,
            ReadOnly,
            Evicted
        }

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void DeleteCountsExistingKey([Values] LogState state, [Values] bool hash)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            if (hash)
            {
                ClassicAssert.IsTrue(db.HashSet("key", "field", "value"));
                ClassicAssert.AreEqual("value", (string)db.HashGet("key", "field"));
            }
            else
            {
                ClassicAssert.IsTrue(db.StringSet("key", "value"));
                ClassicAssert.AreEqual("value", (string)db.StringGet("key"));
            }
            SetLogState(state);

            var deleted = (long)db.Execute("DEL", "key");
            ClassicAssert.IsFalse(db.KeyExists("key"), "Deletion must actually remove the key");
            ClassicAssert.AreEqual(0L, (long)db.Execute("DEL", "key"), "Repeated deletion");
            ClassicAssert.AreEqual(0L, (long)db.Execute("DEL", "absent"), "Absent key");
            ClassicAssert.AreEqual(1L, deleted, "DEL must count the existing key, not just in-place deletions");
        }

        [Test]
        public void DeleteAbsentKeyReturnsZero([Values] LogState state)
        {
            SetLogState(state);
            var db = GetDatabase(out var redis);
            using (redis)
            {
                ClassicAssert.AreEqual(0L, (long)db.Execute("DEL", "absent"));
                ClassicAssert.AreEqual(0L, (long)db.Execute("DEL", "absent"));
            }
        }

        [Test]
        public void MultiKeyDeleteCountsDistinctLiveKeys([Values] LogState state)
        {
            var db = GetDatabase(out var redis);
            using (redis)
            {
                ClassicAssert.IsTrue(db.StringSet("string", "value"));
                ClassicAssert.IsTrue(db.HashSet("hash", "field", "value"));
                SetLogState(state);

                var deleted = (long)db.Execute("DEL", "string", "hash", "string", "absent");
                ClassicAssert.IsFalse(db.KeyExists("string"));
                ClassicAssert.IsFalse(db.KeyExists("hash"));
                ClassicAssert.AreEqual(0L, (long)db.Execute("DEL", "string", "hash"));
                ClassicAssert.AreEqual(2L, deleted);
            }
        }

        [Test]
        public async Task TransactionDeleteCountsExistingKey([Values] LogState state)
        {
            var db = GetDatabase(out var redis);
            using (redis)
            {
                ClassicAssert.IsTrue(db.StringSet("key", "value"));
                SetLogState(state);
                var transaction = db.CreateTransaction();
                var deleted = transaction.ExecuteAsync("DEL", "key");
                var repeated = transaction.ExecuteAsync("DEL", "key");
                ClassicAssert.IsTrue(await transaction.ExecuteAsync());
                ClassicAssert.IsFalse(db.KeyExists("key"));
                ClassicAssert.AreEqual(0L, (long)await repeated);
                ClassicAssert.AreEqual(1L, (long)await deleted);
            }
        }

        [Test]
        public async Task ConcurrentDeletesCountKeyOnce([Values] LogState state)
        {
            var db = GetDatabase(out var redis);
            using (redis)
            using (var other = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                ClassicAssert.IsTrue(db.StringSet("key", "value"));
                SetLogState(state);
                var first = db.ExecuteAsync("DEL", "key");
                var second = other.GetDatabase(0).ExecuteAsync("DEL", "key");
                var replies = await Task.WhenAll(first, second);
                ClassicAssert.IsFalse(db.KeyExists("key"));
                ClassicAssert.AreEqual(1L, (long)replies[0] + (long)replies[1]);
            }
        }

        static IDatabase GetDatabase(out ConnectionMultiplexer redis)
        {
            redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            return redis.GetDatabase(0);
        }

        void SetLogState(LogState state)
        {
            var log = server.Provider.StoreWrapper.store.Log;
            if (state == LogState.ReadOnly)
            {
                log.ShiftReadOnlyAddress(log.TailAddress, wait: true);
            }
            else if (state == LogState.Evicted)
            {
                log.FlushAndEvict(wait: true);
            }
        }
    }
}