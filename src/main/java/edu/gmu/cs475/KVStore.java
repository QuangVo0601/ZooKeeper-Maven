package edu.gmu.cs475;

import edu.gmu.cs475.internal.IKVStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVStore extends AbstractKVStore {

	private HashMap<String, PersistentNode> groupsThatIamIn = new HashMap<>(); //PersistentNode to represent your KVServer in the pool of all active servers

	private HashMap<String, TreeCache> treeCacheHashMap = new HashMap<>(); //to keep track of members in the groups

	private HashMap<String, HashSet<String> > clientKeysHashMap = new HashMap<>(); //list of clients that have cached the keys

	private HashMap<String, ReentrantReadWriteLock> RWLocksMap = new HashMap<>(); //to keep track of handed RWLock

	private ConnectionState currentConnectionState; //to keep track of connection status changes

	private LeaderLatch leaderLatch; //to elect and maintain a leader of the group

	private HashMap<String, String> localValuesCache = new HashMap<>(); //client's local cache
	//	private IKVStore leaderKVStoreConnection;
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
	@Override
	public void initClient(String localClientHostname, int localClientPort) {

		//PersistentNode(CuratorFramework givenClient, org.apache.zookeeper.CreateMode mode, 
		//							boolean useProtection, String basePath, byte[] initData)
		//Create an Ephemeral, PersistentNode to represent your KVServer in the pool of all active servers
		PersistentNode membershipNode = new PersistentNode(zk, CreateMode.EPHEMERAL, false, 
														   ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
		membershipNode.start(); //initiate the persistent node

		groupsThatIamIn.put(ZK_MEMBERSHIP_NODE, membershipNode); 

		//TreeCache to keep track of members in the group
		TreeCache membersInGroupCache = new TreeCache(zk, ZK_MEMBERSHIP_NODE);
		membersInGroupCache.getListenable().addListener((client, event) -> {
			//System.out.println("Client " + debug  + " detected change to membership - " + event);
		});

		try {
			membersInGroupCache.start(); //Start the cache. The cache is not started automatically. You must call this method.
			treeCacheHashMap.put(ZK_MEMBERSHIP_NODE, membersInGroupCache);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		//LeaderLatch(CuratorFramework client, String latchPath, String id)
		leaderLatch = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());

		try {
			leaderLatch.start(); //Add this instance to the leadership election and attempt to acquire leadership.
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Retrieve the value of a key
	 *
	 * @param key the specified key
	 * @return The value of the key or null if there is no such key
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public String getValue(String key) throws IOException {

		String returnValue = "";

		try {
			//Check if this state indicates that Curator has a connection to ZooKeeper (
			if(!currentConnectionState.isConnected()){
				throw new IOException(); //this client is disconnected from ZooKeeper
			}

			Participant currentLeader = leaderLatch.getLeader();

			if(!currentLeader.isLeader()) { //If the leader is disconnected from the client
				//If node N is not able to contact the leader, but does currently hold a live ZooKeeper session (and the leader does not), 
				//then it will first participate in a leader election
				while(!currentLeader.isLeader()){
					currentLeader = leaderLatch.getLeader();
				}

				//When a node detects that the leader has changed (and that it is not the leader), it must flush its entire cache.
				if(!currentLeader.getId().equals(getLocalConnectString())){
					flushClientLocalCache();
				}	
			}

			acquireLock(key).readLock().lock();
			try {
				//try to get the value is in client's local cache
				returnValue = localValuesCache.get(key); 
			}
			finally {
				acquireLock(key).readLock().unlock();
			}

			//if the value is already in client's local cache
			if(currentLeader.getId().equals(getLocalConnectString()) || returnValue != null){
				return returnValue;
			}
			else {
				//connectToKVStore method to connect from one client to another (e.g. to the leader or to a follower)
				IKVStore leaderKVStoreConnection = connectToKVStore(currentLeader.getId());

				//request the value of a key from the leader and other clients
				returnValue = leaderKVStoreConnection.getValue(key, getLocalConnectString());

				//save the new value to local cache if value is not null
				if(returnValue != null){
					acquireLock(key).writeLock().lock();
					try {
						//update client's local cache with new value
						localValuesCache.put(key, returnValue); 
					}
					finally {
						acquireLock(key).writeLock().unlock();
					}
				}	
			}			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException();
		}

		return returnValue;

	}

	/**
	 * Update the value of a key. After updating the value, this new value will be locally cached.
	 *
	 * @param key
	 * @param value
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public void setValue(String key, String value) throws IOException {

		try {

			//Check if this state indicates that Curator has a connection to ZooKeeper
			if(!currentConnectionState.isConnected()){
				throw new IOException(); //this client is disconnected from ZooKeeper
			}

			Participant currentLeader = leaderLatch.getLeader();

			if(!currentLeader.isLeader()) { //If the leader is disconnected from the client
				//If node N is not able to contact the leader, but does currently hold a live ZooKeeper session (and the leader does not), 
				//then it will first participate in a leader election
				while(!currentLeader.isLeader()){
					currentLeader = leaderLatch.getLeader();
				}

				//When a node detects that the leader has changed (and that it is not the leader), it must flush its entire cache.
				if(!currentLeader.getId().equals(getLocalConnectString())){
					flushClientLocalCache();
				}

			}

			if(currentLeader.getId().equals(getLocalConnectString())){
				//Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
				setValue(key, value, getLocalConnectString());	
			}
			else{
				
				IKVStore leaderKVStoreConnection = connectToKVStore(currentLeader.getId());

				//request that the value of a key is updated
				leaderKVStoreConnection.setValue(key, value, getLocalConnectString()); 

				acquireLock(key).writeLock().lock();
				try {
					//update client's local cache with new value
					localValuesCache.put(key, value); 
				}
				finally {
					acquireLock(key).writeLock().unlock();
				}
			}	

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException();
		}
	}

	/**
	 * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 *
	 * @param key    The key requested
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 * @return The value of the key, or null if there is no value for this key
	 * <p>
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public String getValue(String key, String fromID) throws RemoteException {

		String returnValue = "";

		if(leaderLatch.hasLeadership()) { //check if leadership is currently held by this instance

			acquireLock(key).readLock().lock();
			try {
				returnValue = localValuesCache.get(key); //get the value from client's local cache
			}
			finally {
				acquireLock(key).readLock().unlock();
			}

			if(returnValue != null && !fromID.equals(getLocalConnectString())){ //check if requesting node is already in the list of clients having the key
				cacheForSubsequentReads(key, fromID); //The node requesting this value is expected to cache it for subsequent reads.
			}

		}
		return returnValue;
	}

	/**
	 * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 * <p>
	 * This command must wait for any pending writes on the same key to be completed
	 *
	 * @param key    The key to update
	 * @param value  The new value
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 */
	@Override
	public void setValue(String key, String value, String fromID) throws IOException {

		try{
			if(!currentConnectionState.isConnected()){
				throw new IOException();
			}

			//sending invalidate messages
			//When a key’s value is updated, the leader will notify all clients that have a copy of that key to invalidate their cache.
			if(clientKeysHashMap.containsKey(key)){ //only send if the cache has the key
				sendInvalidateMessagesToClients(key, getLocalConnectString(), fromID);
			}

			acquireLock(key).writeLock().lock();
			try {
				localValuesCache.put(key,value); //update client's local cache with new value
			}
			finally {
				acquireLock(key).writeLock().unlock();
			}

			if(!fromID.equals(getLocalConnectString())){ //check if requesting node is already in the list of clients having the key
				cacheForSubsequentReads(key, fromID); //The node requesting this update is expected to cache it for subsequent reads.
			}
		}
		catch(Exception e){
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException();		
		}

	}

	/**
	 * The node requesting a value update is expected to cache it for subsequent reads
	 * @param key 
	 * @param fromID 
	 */
	synchronized protected void cacheForSubsequentReads(String key, String fromID) {

		if(clientKeysHashMap.containsKey(key)){ //check if the key already exists in cache

			synchronized(clientKeysHashMap.get(key)) {
				clientKeysHashMap.get(key).add(fromID); //add the ID of the client making the request to clients cache
			}

		}else{ //if key doesn't exist 

			HashSet<String> valuesSet = new HashSet<>();
			valuesSet.add(fromID); //add the ID of the client making the request to clients cache

			synchronized(clientKeysHashMap) {
				clientKeysHashMap.put(key, valuesSet); //add the new key-value pair to clients cache
			}
		}
	}

	/**
	 * Instruct a node to invalidate any cache of the specified key.
	 * <p>
	 * This method is called BY the LEADER, targeting each of the clients that has cached this key.
	 *
	 * @param key key to invalidate
	 *            <p>
	 *            DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public void invalidateKey(String key) throws RemoteException {

		acquireLock(key).writeLock().lock();
		try {
			localValuesCache.remove(key); //invalidate any cache of the specified key
		}
		finally {
			acquireLock(key).writeLock().unlock();
		}

	}

	/**
	 * When sending invalidate messages, the leader must send these messages to ALL clients with the key cached
	 * When a key’s value is updated, the leader will notify all clients that have a copy of that key to invalidate their cache.
	 * @throws IOException 
	 */
	protected void sendInvalidateMessagesToClients(String key, String myID, String fromID) throws IOException{

		synchronized(clientKeysHashMap.get(key)) {

			HashSet<String> valuesSet = clientKeysHashMap.get(key);

			HashSet<String> failuresSet = new HashSet<>();

			for(String value : valuesSet) {
				//get the clients that are currently in Zookeeper
				Map<String, ChildData> clients = (treeCacheHashMap.get(ZK_MEMBERSHIP_NODE)).getCurrentChildren(ZK_MEMBERSHIP_NODE);

				if(!value.equals(myID) && clients != null && clients.containsKey(value)) {

					try {
						//clients that have a copy of that key to invalidate their cache
						connectToKVStore(value).invalidateKey(key);

					} catch (RemoteException | NotBoundException ex) {
						// TODO Auto-generated catch block
						if(ex instanceof RemoteException){
							failuresSet.add(value); //if there is an exception, save that value for later retry
						}
						ex.printStackTrace();
						throw new IOException();
					}
				}
			}

			//resend invalidate requests to all the failed ones
			if(failuresSet.isEmpty() == false){
				resendInvalidateMessagesToClients(key, failuresSet);
			}

			valuesSet.clear();
		}

	}

	/**
	 * Attempt to send invalidate messages one more time if the previous request failed
	 * @param key to invalidate
	 * @param failuresSet set of previous failed requests
	 * @throws IOException 
	 */
	protected void resendInvalidateMessagesToClients(String key, HashSet<String> failuresSet) throws IOException{

		//resend invalidate requests to all the failed ones
		for(String failure : failuresSet) {

			boolean success = false;

			while(!success){
				//get the clients that are currently in Zookeeper
				Map<String, ChildData> clients = (treeCacheHashMap.get(ZK_MEMBERSHIP_NODE)).getCurrentChildren(ZK_MEMBERSHIP_NODE);

				//if there are active clients is in Zookeeper, and clients have that value cached
				if(clients != null && clients.containsKey(failure)) {

					try{
						//clients that have a copy of that key to invalidate their cache
						connectToKVStore(failure).invalidateKey(key);
						success = true;

					}catch (RemoteException | NotBoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						throw new IOException();
					}
				}
			}
		}
	}

	/**
	 * Called when ZooKeeper detects that your connection status changes
	 * @param curatorFramework
	 * @param connectionState
	 */
	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

		currentConnectionState = connectionState; //get the change in connection status

		if(!currentConnectionState.isConnected()){ //if not actually connected to ZooKeeper
			flushClientLocalCache(); //then flush its entire local cache, including all values and cache information
		}

	}

	/**
	 * Release any ZooKeeper resources that you setup here
	 * (The connection to ZooKeeper itself is automatically cleaned up for you)
	 */
	@Override
	protected void _cleanup() {

		try {
			//If you want to clean up these resources (e.g. call close, in the _cleanup method), 
			//make certain that you only call close if you are actually connected to ZooKeeper, otherwise you will see timeouts!
			if(currentConnectionState.isConnected()) {
				if(zk.getState() != CuratorFrameworkState.STOPPED) { 
					
					//clean up all Zookeeper resources
					groupsThatIamIn.remove(ZK_MEMBERSHIP_NODE).close();
					
					leaderLatch.close(); 
					
					treeCacheHashMap.remove(ZK_MEMBERSHIP_NODE).close();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Flush client's entire local cache, including all values and cache information
	 */
	synchronized protected void flushClientLocalCache(){

		localValuesCache.clear();

	}

	/**
	 * Acquire a ReentrantReadWriteLock for a specific key
	 * @param key
	 * @return ReentrantReadWriteLock for a specific key
	 */
	synchronized protected ReentrantReadWriteLock acquireLock(String key) {

		ReentrantReadWriteLock RWLock = null;

		if(RWLocksMap.containsKey(key)) { //if the RWLock is already hold for a specific key
			RWLock = RWLocksMap.get(key);
		}
		else { //if not hold, create a new RWLock, then save it
			RWLock = new ReentrantReadWriteLock();
			RWLocksMap.put(key, RWLock);		
		}

		return RWLock;

	}
}


