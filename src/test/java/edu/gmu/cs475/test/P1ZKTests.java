package edu.gmu.cs475.test;

import edu.gmu.cs475.AbstractKVStore;
import edu.gmu.cs475.internal.TestingClient;
import org.apache.curator.utils.ZKPaths;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class P1ZKTests extends Base475Test {

	@Rule
	public Timeout globalTimeout = new Timeout(60000);

	@Test
	public void testClientAddsEphemeralMembership() throws Exception {
		TestingClient client = newClient("Leader");
		Assert.assertTrue("Expected a ZKNode created at " + ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, client.getLocalConnectString()), blockUntilMemberJoins(client));
		client.suspendAccessToZK();
		Assert.assertTrue("Expected no ZKNode anymore at " + ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, client.getLocalConnectString()), blockUntilMemberLeaves(client));
	}

	@Test
	public void testFirstClientBecomesLeader() throws Exception {

		TestingClient firstClient = newClient("Leader");
		Assert.assertTrue("Expected the first client we started to assume leadership", blockUntilLeader(firstClient));

		firstClient.suspendAccessToZK();
		Thread.sleep(10000);
		TestingClient second = newClient("Follower");
		Assert.assertTrue("Expected the second client we started to assume leadership", blockUntilLeader(second));

	}
}
