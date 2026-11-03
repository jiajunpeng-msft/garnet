// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A read-only flush copies the page and then writes each record's object to the object log, looking the object up in the live
    /// object map. A copy-update of a read-only record clears its source object as soon as no serialization of that object is in progress,
    /// even if the flush has not reached the record yet. The flush then finds no object for the record, but still gives it an object-log
    /// position and the length of the previous record's object, so the record's on-disk header points at the next record's object bytes.
    /// Reading the page back assigns the wrong objects (or asserts in builds with assertions enabled).
    /// </summary>
    [TestFixture]
    internal class FlushCopyUpdateObjectClearTests : TestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;
        private SerializeGate gate;

        internal sealed class SerializeGate
        {
            internal int blockOnValue = int.MinValue;
            internal readonly ManualResetEventSlim entered = new(false);
            internal readonly ManualResetEventSlim release = new(false);
        }

        sealed class GatedSerializer(SerializeGate gate) : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TestObjectValue { value = reader.ReadInt32() };

            public override void Serialize(IHeapObject obj)
            {
                var value = ((TestObjectValue)obj).value;
                if (value == Volatile.Read(ref gate.blockOnValue))
                {
                    gate.entered.Set();
                    _ = gate.release.Wait(TimeSpan.FromSeconds(30));
                }
                writer.Write(value);
            }
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            gate = new();
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "FlushCopyUpdateObjectClear.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "FlushCopyUpdateObjectClear.obj.log"), deleteOnClose: true);
            var localGate = gate;
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new GatedSerializer(localGate))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            gate?.release.Set();
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
        [Category(ObjectIdMapCategory)]
        public async Task CopyUpdateDuringFlushKeepsObjectLogPositions()
        {
            const int A = 1, B = 2, C = 3, D = 4;
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            var begin = store.Log.TailAddress;
            foreach (var key in new[] { A, B, C, D })
                _ = context.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = 1000 + key }, Empty.Default);
            var flushEnd = store.Log.TailAddress;

            // Flush A..D, holding the flush while it serializes A's object (before it reaches B).
            Volatile.Write(ref gate.blockOnValue, 1000 + A);
            var flush = Task.Run(() => store.Log.ShiftReadOnlyAddress(flushEnd, wait: true));
            Assert.That(gate.entered.Wait(TimeSpan.FromSeconds(10)), Is.True, "The flush did not start serializing A's object");

            // B is now read-only, so this RMW copy-updates it to a new record and clears the source record's object.
            var input = new TestObjectInput { value = 10 };
            var output = new TestObjectOutput();
            var status = context.RMW(new TestObjectKey { key = B }, ref input, ref output, Empty.Default);
            Assert.That(status.Record.CopyUpdated, Is.True, $"Expected a copy-update of B, got {status}");

            gate.release.Set();
            Assert.That(await Task.WhenAny(flush, Task.Delay(TimeSpan.FromSeconds(10))), Is.SameAs(flush), "The flush did not complete");
            await flush;

            // Read every record of the flushed range back from disk.
            store.Log.FlushAndEvict(wait: true);
            var observed = new List<(int key, int value)>();
            using (var iter = store.Log.Scan(begin, flushEnd, DiskScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext())
                {
                    var key = iter.Key.AsRef<TestObjectKey>().key;
                    observed.Add((key, ((TestObjectValue)iter.ValueObject)?.value ?? -1));
                }
            }

            // B's pre-update record is superseded, so it may be omitted, but every record read back must carry its own key's object.
            foreach (var key in new[] { A, C, D })
                ClassicAssert.IsTrue(observed.Exists(o => o.key == key), $"Key {key} missing from the flushed range: [{string.Join(", ", observed)}]");
            foreach (var (key, value) in observed)
                ClassicAssert.AreEqual(1000 + key, value, $"Key {key} read back the wrong object: [{string.Join(", ", observed)}]");
        }
    }
}