package edu.gmu.cs475.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.gmu.cs475.AbstractKVStore;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.netcrusher.NetCrusher;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.ArrayList;

@ShellComponent
public class Command {

	public static TestingServer server;

	static ArrayList<TestingClient> clients = new ArrayList<>();

	public Command() {
		while (server == null) {
			try {
				server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, 500, -1), true);
			} catch (Exception e) {
				System.err.println("Getting bind exception - retrying to allocate server");
				server = null;
			}
		}
		System.out.println("Started ZooKeeper @ port " + server.getPort());
		System.out.println(addClient());
	}

	public static void cleanup() {
		for (TestingClient c : clients) {
			c.cleanup();
		}
		reactor.close();
		try {
			server.stop();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.ERROR);
	}

	private static NioReactor reactor;
	private String addClient() {
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
			if(reactor == null)
				reactor = new NioReactor();

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


			TestingClient c = new TestingClient("localhost:" + lossyZKPort, proxy, rmiProxy, rmiBind, rmiAdvertise, clients.size());
			clients.add(c);
			return "Created client " + (clients.size() - 1) + ", port " + c.getLocalPort();
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}


	@ShellMethod("Create a new client")
	public CharSequence newClient() {
		return addClient();
	}

	@ShellMethod("List clients")
	public CharSequence list() {
		StringBuilder ret = new StringBuilder();
		for (int i = 0; i < clients.size(); i++) {
			ret.append(clients.get(i));
		}
		return ret.toString();
	}

	@ShellMethod("Disable a client's access to ZooKeeper")
	public CharSequence zkDown(int clientID) {
		clients.get(clientID).suspendAccessToZK();
		return clients.get(clientID).toString();
	}

	@ShellMethod("Resume a client's access to ZooKeeper")
	public CharSequence zkUp(int clientID) {
		clients.get(clientID).resumeAccessToZK();
		return clients.get(clientID).toString();
	}


	@ShellMethod("Disable a client's inbound RMI services")
	public CharSequence rmiDown(int clientID) {
		clients.get(clientID).suspendAccessToSelf();
		return clients.get(clientID).toString();
	}

	@ShellMethod("Resume a client's inbound RMI services")
	public CharSequence rmiUp(int clientID) {
		clients.get(clientID).resumeAccessToSelf();
		return clients.get(clientID).toString();
	}

	@ShellMethod("Get a key")
	public CharSequence get(int issueToClient, String key) {
		try {
			return clients.get(issueToClient).getValue(key);
		} catch (Throwable e) {
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}

	@ShellMethod("Set a key")
	public CharSequence put(int issueToClient, String key, String value) {
		try {
			clients.get(issueToClient).setValue(key, value);
			return null;
		} catch (Throwable e) {
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}
}
