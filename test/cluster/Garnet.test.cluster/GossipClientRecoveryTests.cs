// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class GossipClientRecoveryTests
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

        [TestCase(0)]
        [TestCase(1)]
        [CancelAfter(30_000)]
        public async Task GossipRecoversAfterItsClientBecomesTerminallyDisposed(int senderIndex)
        {
            context.CreateInstances(2, enableAOF: true);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId, async: true, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            var sender = (ClusterProvider)context.nodes[senderIndex].Provider.StoreWrapper.clusterProvider;
            var peerId = context.clusterTestUtils.GetNodeIdFromNode(1 - senderIndex, context.logger);
            var store = sender.clusterManager.clusterConnectionStore;
            GarnetServerNode original = null;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            while (!store.GetConnection(peerId, out original) || !original.IsInitialized || original.GetConnectionInfo().pong == 0)
                await Task.Delay(10, deadline.Token);

            var terminalClient = original.Client;
            // TimeoutChecker uses this same terminal transition when an outstanding
            // request makes no progress; avoid an unbounded slow-network fixture.
            terminalClient.Dispose();
            Assert.That(terminalClient.Disposed, Is.True);
            var recovered = false;
            var recoveryDeadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < recoveryDeadline)
            {
                if (store.GetConnection(peerId, out var current)
                    && current.Client.IsConnected && current.IsInitialized && current.GetConnectionInfo().pong > 0)
                {
                    recovered = true;
                    break;
                }
                await Task.Delay(10, deadline.Token);
            }
            Assert.That(recovered, Is.True,
                "The gossip loop must recover a terminally disposed client, not repeatedly reconnect that unusable instance.");

            var primary = context.clusterTestUtils.GetServer(0);
            var replica = context.clusterTestUtils.GetServer(1);
            Assert.That(primary.Execute(0, "SET", ["gossip-recovery-write", "fresh-value"]).ToString(), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            Assert.That(replica.Execute(0, "READONLY", []).ToString(), Is.EqualTo("OK"));
            Assert.That(replica.Execute(0, "GET", ["gossip-recovery-write"], CommandFlags.NoRedirect).ToString(),
                Is.EqualTo("fresh-value"));
            Assert.That(replica.Execute(0, "INFO", ["replication"]).ToString(),
                Does.Contain("master_link_status:up"));
        }
    }
}
