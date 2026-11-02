// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class MigrationBatchBoundaryTests
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

        [Test, CancelAfter(60_000)]
        public async Task DiskBackedMigrationPreservesRejectedBatchBoundaryKeysOnPrimaryAndReplica()
        {
            context.CreateInstances(3, disableObjects: true, enableAOF: true, lowMemory: true, memorySize: "128k", pageSize: "16k");
            context.CreateConnection();
            context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, logger: context.logger);
            for (var i = 0; i < 3; i++)
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 2, logger: context.logger);
            for (var i = 0; i < 3; i++)
                for (var j = 0; j < 3; j++)
                    if (i != j)
                        context.clusterTestUtils.WaitUntilNodeIsKnown(i, j, logger: context.logger);

            var source = context.clusterTestUtils.GetServer(0);
            var target = context.clusterTestUtils.GetServer(1);
            var replica = context.clusterTestUtils.GetServer(2);
            var targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            Assert.That(context.clusterTestUtils.ClusterReplicate(2, targetId, async: true, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(1, 2, context.logger);
            const int count = 3073;
            var keys = new string[count];
            var values = new string[count];
            for (var i = 0; i < count; i++)
            {
                keys[i] = $"{{batch-boundary}}:{i}";
                values[i] = $"value-{i:0000}-".PadRight(256, 'x');
                Assert.That(source.Execute(0, "SET", [keys[i], values[i]]).ToString(), Is.EqualTo("OK"));
            }

            var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            Assert.That(provider.storeWrapper.store.Log.HeadAddress,
                Is.GreaterThan(provider.storeWrapper.store.Log.BeginAddress), "The source must contain cold disk-backed records.");
            provider.serverOptions.ParallelMigrateTaskCount = 1;
            var slot = Garnet.common.HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(keys[0]));
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Assert.That(provider.migrationManager.TryAddMigrationTask(
                null, provider.clusterManager.CurrentConfig.LocalNodeId, endpoint.Address.ToString(), endpoint.Port, targetId,
                null, null, false, false, 30_000, new HashSet<int> { slot },
                null, TransferOption.SLOTS, out var migration), Is.True);

            // Exercise the existing scan continuation contract with several small batches,
            // without a 262144-key fixture or a production-only test hook.
            var operations = (Array)typeof(MigrateSession).GetField("migrateOperation", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(migration);
            foreach (MigrateSession.MigrateOperation operation in operations)
                typeof(Sketch).GetField(nameof(Sketch.argSliceVector)).SetValue(operation.sketch, new ArgSliceVector(1024));

            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            Assert.That((await migration.TryStartMigrationTaskAsync()).Success, Is.True);
            while (provider.migrationManager.GetMigrationTaskCount() != 0)
                await Task.Delay(10, deadline.Token);
            Assert.That(migration.Status, Is.EqualTo(MigrateState.SUCCESS));
            context.clusterTestUtils.WaitForReplicaAofSync(1, 2, context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(2, targetId, [slot, slot], context.logger);
            Assert.That(replica.Execute(0, "READONLY", []).ToString(), Is.EqualTo("OK"));

            for (var i = 0; i < count; i++)
            {
                Assert.That(target.Execute(0, "GET", [keys[i]], CommandFlags.NoRedirect).ToString(),
                    Is.EqualTo(values[i]), $"Target lost source ordinal {i + 1}.");
                Assert.That(replica.Execute(0, "GET", [keys[i]], CommandFlags.NoRedirect).ToString(),
                    Is.EqualTo(values[i]), $"Replica lost source ordinal {i + 1}.");
            }
        }
    }
}
