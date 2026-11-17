// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class MigrationCollisionTests
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        [TestCase("SET")]
        [TestCase("HSET")]
        [CancelAfter(30_000)]
        public async Task AbsentKeySketchCollisionDoesNotWaitForUnrelatedMigration(string command)
        {
            context.CreateInstances(2, disableObjects: false, enableAOF: true);
            context.CreateConnection();
            context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(0, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, logger: context.logger);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(1, 0, logger: context.logger);

            var storedKey = Encoding.ASCII.GetBytes("{migration-collision}:stored");
            const int sketchSize = 64;
            var bit = (int)HashUtils.MurmurHash2x64A(storedKey) & (sketchSize - 1);
            byte[] absentKey = null;
            for (var i = 0; i < 10_000; i++)
            {
                var candidate = Encoding.ASCII.GetBytes($"{{migration-collision}}:absent:{i}");
                if (((int)HashUtils.MurmurHash2x64A(candidate) & (sketchSize - 1)) == bit)
                {
                    absentKey = candidate;
                    break;
                }
            }
            Assert.That(absentKey, Is.Not.Null, "A bounded deterministic search must find a bitmap collision.");
            var slot = HashSlotUtils.HashSlot(storedKey);
            Assert.That(HashSlotUtils.HashSlot(absentKey), Is.EqualTo(slot));

            var source = context.clusterTestUtils.GetServer(0);
            Assert.That(source.Execute(0, "SET", [storedKey, "held-migration-value"]).ToString(), Is.EqualTo("OK"));
            Assert.That(source.Execute(0, "EXISTS", [absentKey]).ToString(), Is.EqualTo("0"));
            var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            var original = provider.clusterManager.CurrentConfig;
            var targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            var pin = GCHandle.Alloc(storedKey, GCHandleType.Pinned);
            var sketch = new Sketch(sketchSize);
            sketch.HashAndStore(Pinned(pin.AddrOfPinnedObject(), storedKey.Length));
            sketch.SetStatus(SketchStatus.TRANSMITTING);
            MigrateSession migration = null;
            using var connection = new TcpClient();
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(0));
            using var stream = connection.GetStream();
            var response = new byte[256];
            Task<int> pending = null;
            try
            {
                Assert.That(provider.migrationManager.TryAddMigrationTask(
                    null, original.LocalNodeId, endpoint.Address.ToString(), endpoint.Port, targetId,
                    null, null, false, false, 5_000, new HashSet<int> { slot },
                    sketch, TransferOption.KEYS, out migration), Is.True);
                provider.clusterManager.UnsafeSetConfig(
                    original.UpdateSlotState(slot, original.GetWorkerIdFromNodeId(targetId), SlotState.MIGRATING));
                Assert.That(provider.migrationManager.CanAccessKey(Pinned(pin.AddrOfPinnedObject(), storedKey.Length), slot, false),
                    Is.False, "The real transmitting key must remain write-protected.");

                // Hold the real migration gate without a wall-clock-dependent slow network or large preload.
                // An absent collider is not part of this batch and should receive ASK immediately.
                string[] arguments = command == "HSET"
                    ? [command, Encoding.ASCII.GetString(absentKey), "field", "value"]
                    : [command, Encoding.ASCII.GetString(absentKey), "value"];
                var request = new StringBuilder($"*{arguments.Length}\r\n");
                foreach (var argument in arguments)
                    request.Append($"${argument.Length}\r\n{argument}\r\n");
                await stream.WriteAsync(Encoding.ASCII.GetBytes(request.ToString()));
                pending = stream.ReadAsync(response).AsTask();
                var completed = await Task.WhenAny(pending, Task.Delay(1_000));
                Assert.That(completed, Is.SameAs(pending),
                    "A write to an absent, non-batch key must not wait for an unrelated transmitting batch.");
                Assert.That(Encoding.ASCII.GetString(response, 0, await pending),
                    Does.StartWith($"-ASK {slot} "));
            }
            finally
            {
                sketch.SetStatus(SketchStatus.MIGRATED);
                try
                {
                    if (pending != null)
                    {
                        _ = await pending.WaitAsync(TimeSpan.FromSeconds(5));
                    }
                }
                finally
                {
                    provider.clusterManager.UnsafeSetConfig(original);
                    if (migration != null)
                        Assert.That(provider.migrationManager.TryRemoveMigrationTask(migration), Is.True);
                    pin.Free();
                }
            }
        }

        static unsafe PinnedSpanByte Pinned(IntPtr pointer, int length)
            => PinnedSpanByte.FromPinnedPointer((byte*)pointer, length);
    }
}
