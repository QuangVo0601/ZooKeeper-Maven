package edu.gmu.cs475.internal;

import edu.gmu.cs475.KVStore;
import org.netcrusher.NetCrusher;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.function.Function;

public class TestingClient extends KVStore {

	public LinkedList<String> getValueServerInvokes = new LinkedList<>();
	public LinkedList<String> setValueServerInvokes = new LinkedList<>();
	public LinkedList<String> invalidateKeyInvokes = new LinkedList<>();
	boolean zkDown;
	boolean rmiDown;
	private String toString;
	private Function<String, Void> invalidateHandler;
	private Function<String, Void> getHandler;
	private NetCrusher proxyToZK;
	private NetCrusher proxyToSelf;

	private int debug;

	public TestingClient(String zkConnectString, NetCrusher proxyToZK, NetCrusher proxyToSelf, int rmiBind, int rmiAdvertise, int debug) {
		super();
		this.debug = debug;
		this.proxyToSelf = proxyToSelf;
		this.proxyToZK = proxyToZK;
		init(zkConnectString, rmiBind, rmiAdvertise, debug);
	}

	public void setGetHandler(Function<String, Void> getHandler) {
		this.getHandler = getHandler;
	}

	public void setInvalidateHandler(Function<String, Void> handler) {
		this.invalidateHandler = handler;
	}

	public void setToString(String toString) {
		this.toString = toString;
	}

	@Override
	public String getValue(String key, String fromID) throws RemoteException {
		if (rmiDown)
			try {
				synchronized (this) {
					this.wait();
				}
			} catch (InterruptedException ex) {
			}
		getValueServerInvokes.add(key + "," + fromID);
		String ret = super.getValue(key, fromID);
		if (getHandler != null)
			getHandler.apply(key);
		return ret;
	}

	@Override
	public void setValue(String key, String value, String fromID) throws IOException{
		if (rmiDown)
			try {
				synchronized (this) {
					this.wait();
				}
			} catch (InterruptedException ex) {
			}
		setValueServerInvokes.add(key + "," + value + "," + fromID);
		super.setValue(key, value, fromID);
	}

	@Override
	public void invalidateKey(String key) throws RemoteException {
		if (rmiDown)
			try {
				synchronized (this) {
								this.wait();
				}
			} catch (InterruptedException ex) {
			}
		invalidateKeyInvokes.add(key);
		if (invalidateHandler != null)
			invalidateHandler.apply(key);
		super.invalidateKey(key);
	}

	public void suspendAccessToZK() {
		this.proxyToZK.freeze();
		zkDown = true;
	}

	public void resumeAccessToZK() {
		zkDown = false;
		this.proxyToZK.unfreeze();
	}

	public void suspendAccessToSelf() {
		rmiDown = true;
		this.proxyToSelf.freeze();
	}

	public void resumeAccessToSelf() {
		rmiDown = false;
		synchronized (this){
			this.notifyAll();
		}
		this.proxyToSelf.unfreeze();
	}

	public void _cleanup() {
		super._cleanup();
		proxyToSelf.close();
		proxyToZK.close();
	}

	@Override
	public String toString() {
		if (toString != null)
			return toString;
		return "Client #" + debug + " @port" + getLocalPort() + ", connected to ZooKeeper " + (zkDown ? "N" : "Y") + ", listening on RMI " + (rmiDown ? "N" : "Y");
	}
}
