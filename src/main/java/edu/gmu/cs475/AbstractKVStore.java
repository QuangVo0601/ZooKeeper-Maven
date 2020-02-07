package edu.gmu.cs475;

import edu.gmu.cs475.internal.IKVStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public abstract class AbstractKVStore implements IKVStore, ConnectionStateListener {

	public final static String ZK_MEMBERSHIP_NODE = "/edu/gmu/cs475/membership";
	public final static String ZK_LEADER_NODE = "/edu/gmu/cs475/leader";
	public CuratorFramework zk;
	HashMap<String, IKVStore> connectedTo = new HashMap<>();
	private int localPort;
	private Registry rmiRegistry;

	public AbstractKVStore(){

	}


	/**
	 * Internal method used to set up all of the connections
	 * @param zkConnectString
	 */
	public final void init(String zkConnectString) {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			localPort = socket.getLocalPort();
		} catch (Exception ex) {
			throw new IllegalStateException(ex);
		}
		startReplica();
		zk = CuratorFrameworkFactory.newClient(zkConnectString, new RetryNTimes(0, 3));
		zk.getConnectionStateListenable().addListener(this);
		zk.start();
		try {
			zk.blockUntilConnected();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		initClient("localhost", localPort);
	}

	/**
	 * Internal method used to set up all of the connections
	 * @param zkConnectString
	 */
	public final void init(String zkConnectString, int portToBindTo, int portToAdvertise, int debug) {
		localPort = portToBindTo;
		startReplica();
		localPort = portToAdvertise;
		zk = CuratorFrameworkFactory.newClient(zkConnectString, (v1,v2,v3)->{
			return false;
		});
		zk.getConnectionStateListenable().addListener(this);
		zk.start();
		try {
			zk.blockUntilConnected();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		initClient("localhost", localPort);
	}

	public int getLocalPort() {
		return localPort;
	}

	public String getLocalConnectString() {
		return "localhost" + ":" + getLocalPort();
	}

	protected IKVStore connectToKVStore(String connectionString) throws RemoteException, NotBoundException {
		String[] d = connectionString.split(":");
		synchronized (connectedTo) {
			if (!connectedTo.containsKey(connectionString)) {
				Registry registry = LocateRegistry.getRegistry(d[0], Integer.valueOf(d[1]));
				IKVStore kvs = (IKVStore) registry.lookup(IKVStore.RMI_NAME);
				connectedTo.put(connectionString, kvs);
			}
			return connectedTo.get(connectionString);
		}
	}

	protected final void startReplica() {
		try {
			rmiRegistry = LocateRegistry.createRegistry(localPort);
			IKVStore replica = (IKVStore) UnicastRemoteObject.exportObject(this, 0);
			rmiRegistry.rebind(IKVStore.RMI_NAME, replica);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Cleans up the RMI sever that's running the cache client
	 */
	public final void cleanup() {
		try {
			_cleanup();
			UnicastRemoteObject.unexportObject(this, true);
			rmiRegistry.unbind(IKVStore.RMI_NAME);
			zk.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	protected abstract void _cleanup();


	/**
	 * This callback is invoked once your client has started up and published an RMI endpoint.
	 * <p>
	 * In this callback, you will need to set-up your ZooKeeper connections, and then publish your
	 * RMI endpoint into ZooKeeper (publishing the hostname and port)
	 * <p>
	 * You will also need to set up any listeners to track ZooKeeper events
	 *
	 * @param localClientHostname Your client's hostname, which other clients will use to contact you
	 * @param localClientPort     Your client's port number, which other clients will use to contact you
	 */
	public abstract void initClient(String localClientHostname, int localClientPort);

	/**
	 * Retrieve the value of a key
	 *
	 * @param key
	 * @return The value of the key or null if there is no such key
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	public abstract String getValue(String key) throws IOException;

	/**
	 * Update the value of a key. After updating the value, this new value will be locally cached.
	 *
	 * @param key
	 * @param value
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	public abstract void setValue(String key, String value) throws IOException;


}
