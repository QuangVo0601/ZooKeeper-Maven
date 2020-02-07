package edu.gmu.cs475.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.gmu.cs475.AbstractKVStore;
import edu.gmu.cs475.internal.TestingClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;


public class Base475Test {
	protected static final int RETRIES = 5;
	protected static final int RETRIES_SESSION_END = 15;
	protected static final long WAIT_TIME = 1000;
	protected static final long WAIT_TIME_SESSION_END = 5000;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.ERROR);
	}

	protected TestingServer server;
	protected CuratorFramework testingCurator;
	protected ZooKeeper zk;
	ArrayList<TestingClient> clients;


	int keyIdx;
	int valIdx;

	protected String getNewKey() {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String callingMethod = stackTraceElements[2].getMethodName();
		keyIdx++;
		return callingMethod + "-key-" + keyIdx;
	}

	protected String getNewValue() {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		String callingMethod = stackTraceElements[2].getMethodName();
		valIdx++;
		return callingMethod + "-value-" + valIdx;
	}

	protected void assertGetValueCalled(TestingClient c, int min, int max) {
		boolean ok = c.getValueServerInvokes.size() >= min && c.getValueServerInvokes.size() <= max;
		Assert.assertTrue("Expected " + (min == max ? min : "(" + min + "-" + max + ")") + " getValue calls on client " + c.toString() + ", got" + c.getValueServerInvokes, ok);
		c.getValueServerInvokes.clear();
	}

	protected void assertSetValueCalled(TestingClient c, int min, int max) {
		boolean ok = c.setValueServerInvokes.size() >= min && c.setValueServerInvokes.size() <= max;
		Assert.assertTrue("Expected " + (min == max ? min : "(" + min + "-" + max + ")") + " setValue calls on client " + c.toString() + ", got " + c.setValueServerInvokes, ok);
		c.setValueServerInvokes.clear();
	}

	protected void assertInvalidateCalled(TestingClient c, int min, int max) {
		boolean ok = c.invalidateKeyInvokes.size() >= min && c.invalidateKeyInvokes.size() <= max;
		Assert.assertTrue("Expected " + (min == max ? min : "(" + min + "-" + max + ")") + " invalidate calls on client " + c.toString() + ", got " + c.invalidateKeyInvokes, ok);
		c.invalidateKeyInvokes.clear();
	}

	protected void assertGetValueCalled(TestingClient c, String key, String caller) {
		Assert.assertTrue("Expected getValue called with " + key + "," + caller + ", but no call found", c.getValueServerInvokes.size() > 0);
		String v = c.getValueServerInvokes.pop();
		Assert.assertEquals("Expected getValue to be called on server, found most recently: " + v + ", plus: " + c.getValueServerInvokes, key + "," + caller, v);
	}

	protected void assertSetValueCalled(TestingClient c, String key, String value, String caller) {
		Assert.assertTrue("Expected setValue called with " + key + "," + value + "," + caller + ", but no call found", c.setValueServerInvokes.size() > 0);
		String v = c.setValueServerInvokes.pop();
		Assert.assertEquals("Expected setValue to be called on server, found most recently: " + v + ", plus: " + c.setValueServerInvokes, key + "," + value + "," + caller, v);
	}

	protected void assertInvalidateCalled(TestingClient c, String key) {
		Assert.assertTrue("Expected invalidate called with " + key + ", but no call found", c.invalidateKeyInvokes.size() > 0);
		String v = c.invalidateKeyInvokes.pop();
		Assert.assertEquals("Expected invalidate to be called on client " + c + ", found most recently: " + v + ", plus: " + c.invalidateKeyInvokes, key, v);
	}

	protected void setKeyAndRead(boolean expectInvalidate, String key, String value, TestingClient ldr, TestingClient writeTo, TestingClient... otherClients) throws IOException {
		writeTo.setValue(key, value);
		assertSetValueCalled(ldr, key, value, writeTo.getLocalConnectString()); //Even if you're the leader, you need to execute this logic

		Assert.assertEquals(value, writeTo.getValue(key));
		assertGetValueCalled(ldr, 0, 0); //value must be cached on this node now

		//Make sure write is visible everywhere else
		Assert.assertEquals("Expectected getValue to be correct on leader ", value, ldr.getValue(key));
		assertGetValueCalled(ldr, 0, 1); //Allowed to cache, not mandatory to cache this

		for (TestingClient client : otherClients) {
			Assert.assertEquals("Expected getValue to be correct on client" + client, value, client.getValue(key));
			assertGetValueCalled(ldr, key, client.getLocalConnectString());
		}
		assertSetValueCalled(ldr, 0, 0);
		for (TestingClient client : otherClients) {
			if (expectInvalidate)
				assertInvalidateCalled(client, key);
			assertInvalidateCalled(client, 0, 0);
		}
		if (expectInvalidate)
			assertInvalidateCalled(writeTo, 0, 1);
		else
			assertInvalidateCalled(writeTo, 0, 0);
	}


	protected TestingClient newClient(String debugStr) {

		int lossyZKPort = 0;
		int rmiBind = 0;
		int rmiAdvertise = 0;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			lossyZKPort = socket.getLocalPort();
		} catch (Exception ex) {

		}
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			rmiBind = socket.getLocalPort();
		} catch (Exception ex) {

		}
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			rmiAdvertise = socket.getLocalPort();
		} catch (Exception ex) {

		}
		try {
			NioReactor reactor = new NioReactor();

			TcpCrusher proxy = TcpCrusherBuilder.builder()
					.withReactor(reactor)
					.withBindAddress("localhost", lossyZKPort)
					.withConnectAddress("localhost", server.getPort())
					.buildAndOpen();
			TcpCrusher rmiProxy = TcpCrusherBuilder.builder()
					.withReactor(reactor)
					.withBindAddress("localhost", rmiAdvertise)
					.withConnectAddress("localhost", rmiBind)
					.buildAndOpen();

//			TestingClient c = EasyMock
//					.partialMockBuilder(TestingClient.class)
//					.withConstructor("localhost:" + lossyZKPort, proxy, rmiProxy, rmiBind, rmiAdvertise, clients.size())
//					.createMock();

			TestingClient c = new TestingClient("localhost:" + lossyZKPort, proxy, rmiProxy, rmiBind, rmiAdvertise, clients.size());
			c.setToString(debugStr + " [#" + clients.size() + ", localhost:" + rmiAdvertise + "]");
			clients.add(c);
			return c;
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public boolean blockUntilMemberJoins(TestingClient p) throws InterruptedException, KeeperException {
		String member = "localhost:" + p.getLocalPort();
		Stat node = null;
		int nTried = 0;
		boolean found = false;
		while (nTried < RETRIES && !found) {
			node = zk.exists(ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, member), false);
			if (node != null)
				found = true;
			if (!found)
				Thread.sleep(WAIT_TIME);
			nTried++;
		}
		return found;
	}

	public boolean blockUntilMemberLeaves(TestingClient p) throws InterruptedException, KeeperException {
		String member = "localhost:" + p.getLocalPort();
		Stat node = null;
		int nTried = 0;
		boolean found = true;
		while (nTried < RETRIES_SESSION_END && found) {
			node = zk.exists(ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, member), false);
			if (node == null)
				found = false;
			if (found)
				Thread.sleep(WAIT_TIME_SESSION_END);
			nTried++;
		}
		return !found;
	}

	protected boolean blockUntilLeader(TestingClient p) throws InterruptedException {
		String id = p.getLocalConnectString();
		LeaderLatch leadership = new LeaderLatch(testingCurator, AbstractKVStore.ZK_LEADER_NODE);
		int nTried = 0;
		boolean found = false;
		String leaderID = null;
		while (nTried < RETRIES && (leaderID == null || !leaderID.equals(p.getLocalConnectString()))) {
			Thread.sleep(WAIT_TIME);
			try {
				Participant curLeader = leadership.getLeader();
				if (curLeader.getId().length() > 0)
					leaderID = curLeader.getId();
			} catch (Exception ex) {
			}
			nTried++;
		}
		return id.equals(leaderID);
	}

	protected TestingClient blockUntilLeader(TestingClient... p) throws InterruptedException {
		HashSet<String> ids = new HashSet<>();
		for(TestingClient _p : p)
			ids.add(_p.getLocalConnectString());
		LeaderLatch leadership = new LeaderLatch(testingCurator, AbstractKVStore.ZK_LEADER_NODE);
		int nTried = 0;
		boolean found = false;
		String leaderID = null;
		while (nTried < RETRIES && (leaderID == null || !ids.contains(leaderID))){
			Thread.sleep(WAIT_TIME);
			try {
				Participant curLeader = leadership.getLeader();
				if (curLeader.getId().length() > 0)
					leaderID = curLeader.getId();
			} catch (Exception ex) {
			}
			nTried++;
		}
		for(TestingClient _p : p)
			if(_p.getLocalConnectString().equals(leaderID))
				return _p;
		return null;
	}

	protected boolean blockUntilNotLeader(TestingClient p) throws InterruptedException {
		String id = p.getLocalConnectString();
		LeaderLatch leadership = new LeaderLatch(testingCurator, AbstractKVStore.ZK_LEADER_NODE);
		int nTried = 0;
		boolean found = false;
		String leaderID = null;
		while (nTried < RETRIES && (leaderID != null && leaderID.equals(p.getLocalConnectString()))) {
			Thread.sleep(WAIT_TIME);
			try {
				Participant curLeader = leadership.getLeader();
				if (curLeader.getId().length() > 0)
					leaderID = curLeader.getId();
			} catch (Exception ex) {
			}
			nTried++;
		}
		return !id.equals(p.getLocalConnectString());
	}

	@Before
	public void setupZK() throws Exception {
		keyIdx = 0;
		valIdx = 0;
		while (server == null) {
			try {
				server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, 500, -1), true);
			} catch (BindException e) {
				System.err.println("Getting bind exception - retrying to allocate server");
				server = null;
			}
		}

		clients = new ArrayList<>();
		testingCurator = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
		testingCurator.start();
		testingCurator.blockUntilConnected();
		zk = testingCurator.getZookeeperClient().getZooKeeper();

	}

	@After
	public void teardown() {
		testingCurator.close();
		for (TestingClient c : clients)
			c.cleanup();
		if (server != null) {
			try {
				server.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				server = null;
			}
		}
	}
}
