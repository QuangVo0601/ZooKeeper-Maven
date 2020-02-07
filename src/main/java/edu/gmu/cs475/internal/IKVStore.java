package edu.gmu.cs475.internal;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

public interface IKVStore extends Remote{
	public static final String RMI_NAME = "IKVStore";


	/**
	 * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
	 *
	 * This command should ONLY be called as a request to the leader.
	 *
	 * @param key The key requested
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 * @return The value of the key, or null if there is no value for this key
	 *
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	public String getValue(String key, String fromID) throws RemoteException;

	/**
	 * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
	 *
	 * This command should ONLY be called as a request to the leader.
	 *
	 * This command must wait for any pending writes on the same key to be completed
	 *
	 * @param key The key to update
	 * @param value The new value
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 *
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	public void setValue(String key, String value, String fromID) throws RemoteException, IOException;

	/**
	 * Instruct a node to invalidate any cache of the specified key.
	 *
	 * This method is called BY the LEADER, targeting each of the clients that has cached this key.
	 *
	 * @param key key to invalidate
	 *
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	public void invalidateKey(String key) throws RemoteException;

}
