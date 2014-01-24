/**
 * File: MessagePasser.java
 * @author Ashish Kaila
 * @author Ying Li
 * @since  January 18th 2014
 *
 * Brief: Message Passing interface. This is the class that is responsible
 * for managing connections and sending/receiving messages from other nodes.
 */
package lab0;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.yaml.snakeyaml.Yaml;

public class MessagePasser {

	private File config;
	private long configLastModifiedTime;
	private String name;
	private AtomicInteger id;
	private BlockingQueue<Message> sendDelayQueue;
	private BlockingQueue<Message> rcvQueue;
	private BlockingQueue<Message> rcvDelayQueue;
	private HashMap<String, ObjectOutputStream> connectionCache;

	public HashMap<String, Node> nodeMap = null;
	public ArrayList<Rule> sendRules = null;
	public ArrayList<Rule> rcvRules = null;

	public MessagePasser(String configFile, String name) {
		config = new File(configFile);
		if (!config.exists() || config.isDirectory()) {
			CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
		}
		this.name = new String(name);
		nodeMap = new HashMap<String, Node>();

		parseConfig();

		/* initialize state for message passer */
		id = new AtomicInteger(-1);
		setSendDelayQueue(new LinkedBlockingQueue<Message>());
		setRcvQueue(new LinkedBlockingQueue<Message>());
		setRcvDelayQueue(new LinkedBlockingQueue<Message>());
		setConnectionCache(new HashMap<String, ObjectOutputStream>());

		/* create server thread for this message passer object */
		ServerThread server = new ServerThread(this);
		new Thread(server).start();
	}

	@SuppressWarnings("unchecked")
	public int generateSendRules() {
		FileInputStream configFileStream = null;
		ArrayList<Rule> sendRules = null;

		try {
			configFileStream = new FileInputStream(config);
			Yaml yaml = new Yaml();
			sendRules = new ArrayList<Rule>();

			Map<String, Object> configMap = (Map<String, Object>) yaml.load(configFileStream);
			if (configMap.isEmpty() || configMap.keySet().size() > Constants.CONFIG_PARAMS) {
				return -1;
			}

			/* read the send rules from configuration file
			 *    - can have an empty send rules section or unspecified altogether
			 */
			List<Map<String, Object>> sendRuleList = (List<Map<String, Object>>)
					configMap.get("sendRules");
			if (sendRuleList == null) {
				return 0;
			}
			for (Map<String, Object> iterator : sendRuleList) {
				String action = (String)iterator.get("action");
				if (action == null || !(action.equalsIgnoreCase("drop")
						|| action.equalsIgnoreCase("duplicate")
						|| action.equalsIgnoreCase("delay"))) {
					return -1;
				}
				Rule newRule = new Rule(action);
				newRule.setDest((String)iterator.get("dest"));
				newRule.setSrc((String)iterator.get("src"));
				newRule.setKind((String)iterator.get("kind"));
				newRule.setSeqnum((Integer)iterator.get("seqNum"));
				sendRules.add(newRule);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Config file not found " + config);
			return -1;
		} catch (Exception e) {
			System.err.println("Unable to generate send rules from config: " + config);
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					System.err.println("SendRules: Unable to close config stream");
					return -1;
				}
			}
		}

		/* if parsing the configuration worked update send rules */
		this.sendRules = sendRules;
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int generateRcvRules() {
		FileInputStream configFileStream = null;
		ArrayList<Rule> rcvRules = null;

		try {
			configFileStream = new FileInputStream(config);
			Yaml yaml = new Yaml();
			rcvRules = new ArrayList<Rule>();

			Map<String, Object> configMap = (Map<String, Object>) yaml.load(configFileStream);
			if (configMap.isEmpty() || configMap.keySet().size() > Constants.CONFIG_PARAMS) {
				return -1;
			}

			/* read the receive rules from configuration file
			 *    - can have an empty receive rules section or unspecified altogether
			 */
			List<Map<String, Object>> rcvRuleList = (List<Map<String, Object>>)
					configMap.get("receiveRules");
			if (rcvRuleList == null) {
				return 0;
			}
			for (Map<String, Object> iterator : rcvRuleList) {
				String action = (String)iterator.get("action");
				if (action == null || !(action.equalsIgnoreCase("drop")
						|| action.equalsIgnoreCase("duplicate")
						|| action.equalsIgnoreCase("delay"))) {
					return -1;
				}
				Rule newRule = new Rule(action);
				newRule.setDest((String)iterator.get("dest"));
				newRule.setSrc((String)iterator.get("src"));
				newRule.setKind((String)iterator.get("kind"));
				newRule.setSeqnum((Integer)iterator.get("seqNum"));
				rcvRules.add(newRule);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Config file not found " + config);
			return -1;
		} catch (Exception e) {
			System.err.println("Unable to generate receive rules from config: " + config);
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					System.err.println("RcvRules: Unable to close config stream");
					return -1;
				}
			}
		}

		/* if parsing the configuration worked update receive rules */
		this.rcvRules = rcvRules;
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int parseConfig() {
		FileInputStream configFileStream = null;
		configLastModifiedTime = config.lastModified();

		try {
			configFileStream = new FileInputStream(config);
			Yaml yaml = new Yaml();
			Map<String, Object> configMap = (Map<String, Object>) yaml.load(configFileStream);
			if (configMap.isEmpty() || configMap.keySet().size() > Constants.CONFIG_PARAMS) {
				CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
				return -1;
			}

			/* parse configuration parameter from configuration file */
			List<Map<String, Object>> configList = (List<Map<String, Object>>)
					configMap.get("configuration");
			if (configList == null || configList.size() == 0) {
				CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
				return -1;
			}
			for (Map<String, Object> iterator : configList) {
				String name = (String) iterator.get("name");
				String ip = (String) iterator.get("ip");
				Integer port = (Integer)iterator.get("port");

				if (name == null || ip == null || port == null) {
					CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
					return -1;
				}

				Node newNode = new Node(name, ip, port);
				if (nodeMap.containsKey(name)) {
					CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
					return -1;
				}
				nodeMap.put(name, newNode);
			}

			if (!nodeMap.containsKey(this.name)) {
				CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
				return -1;
			}

			if (generateSendRules() < 0) {
				CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
				return -1;
			}

			if (generateRcvRules() < 0) {
				CommunicationInfra.usage(Constants.INVALID_CONFIG_PARAMS);
				return -1;
			}

		}  catch (FileNotFoundException e) {
			CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
			return -1;
		} catch (Exception e) {
			CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					System.err.println("parseConfig: Unable to close config stream");
					CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
					return -1;
				}
			}
		}

		return 0;
	}

	/* This method should be called with locks held */
	public Rule getSendRule(Message msg) {
		Rule matchedRule = null;
		for (Rule rule : sendRules) {
			if (rule.matches(msg)) {
				matchedRule = rule;
				break;
			}
		}

		return matchedRule;
	}

	/* This method should be called with locks held */
	public Rule getRcvRule(Message msg) {
		Rule matchedRule = null;
		for (Rule rule : rcvRules) {
			if (rule.matches(msg)) {
				matchedRule = rule;
				break;
			}
		}

		return matchedRule;
	}

	/* This method should always be synchronized */
	public synchronized void updateConfig() {
		long modifiedTime = 0;

		/* reload rules if configuration file has been modified */
		modifiedTime = this.config.lastModified();

		if (this.configLastModifiedTime < modifiedTime) {
			generateSendRules();
			generateRcvRules();
			this.configLastModifiedTime = modifiedTime;
			}
	}

	public void send(Message message) {
		Rule msgRule = null;

		if (message == null || message.getDest() == null) {
			return;
		}

		if (nodeMap.get(message.getDest()) == null) {
			System.err.println("Tried sending message to unknown host " + message.getDest());
			return;
		}

		message.setId(id.incrementAndGet());
		message.setSrc(this.name);

		synchronized(this) {
			this.updateConfig();
			msgRule = getSendRule(message);
		}

		/* Check to see what the rules say needs to be done with this message */
		try {
			if (msgRule != null) {
				if (msgRule.getAction().equals("drop")) {
					/* forget the message */
					message = null;
					return;
				} else if (msgRule.getAction().equals("delay")) {
					/* add message to delay queue, it will be acted on later */
					synchronized(this) {
						this.sendDelayQueue.add(message);
					}
					return;
				} else if (msgRule.getAction().equals("duplicate")) {
					/* send a duplicate of the message */
					actuallySend(message);
					message.setDuplicate(true);
				}
			}

			/* now send the message(s) */
			actuallySend(message);

			synchronized(this) {
				while (!this.sendDelayQueue.isEmpty()) {
					actuallySend(sendDelayQueue.remove());
				}
			}

		}  catch (NoSuchElementException e){
			System.err.println("send: Delay queue empty");
		} catch (Exception e) {
			System.err.println("send: Exception while sending message");
		}
	}

	private void actuallySend(Message message) {
		ObjectOutputStream objOpStream = null;
		Socket sock = null;

		if (message == null) {
			return;
		}

		try {
			/* lookup the cached connection */
			synchronized(this) {
				objOpStream = this.getConnectionCache().get(message.getDest());
				if (objOpStream == null) {
					sock = new Socket(nodeMap.get(message.getDest()).getIp(),
							nodeMap.get(message.getDest()).getPort());
					objOpStream = new ObjectOutputStream(sock.getOutputStream());
					this.getConnectionCache().put(message.getDest(), objOpStream);
				}

				objOpStream.writeObject(message);
				objOpStream.flush();
				objOpStream.reset();
			}
		} catch (UnknownHostException e) {
			System.err.println("Unable to connect to host " + message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}

				if (tmpStream != null) {
					tmpStream.close();
				}

				if (sock != null) {
					sock.close();
				}
			} catch (Exception ex){
				System.err.println("actuallySend: Error while closing output stream");
			}
		} catch (SocketException e) {
			System.err.println("Unable to create or access Socket to destination "
			+ message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}

				if (tmpStream != null) {
					tmpStream.close();
				}

				if (sock != null) {
					sock.close();
				}
			} catch (Exception ex){
				System.err.println("actuallySend: Error while closing output stream");
			}
		} catch (IOException e) {
			System.err.println("An I/O exception occurred while sending to destination "
			+ message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}

				if (tmpStream != null) {
					tmpStream.close();
				}

				if (sock != null) {
					sock.close();
				}
			} catch (Exception ex){
				System.err.println("actuallySend: Error while closing output stream");
			}
		}
	}

	public Message receive() {
		Message message = null;

		/* update the rules if configuration file was changed */
		synchronized(this) {
			this.updateConfig();
		}

		try {
			message = this.rcvQueue.take();
		} catch (InterruptedException e) {
			System.err.println("recevie: Block on receive was interrupted");
		}
		return message;
	}

	public File getConfig() {
		return config;
	}

	public void setConfig(File config) {
		this.config = config;
	}

	public long getConfigLastModifiedTime() {
		return configLastModifiedTime;
	}

	public void setConfigLastModifiedTime(long configLastModifiedTime) {
		this.configLastModifiedTime = configLastModifiedTime;
	}

	public BlockingQueue<Message> getRcvQueue() {
		return rcvQueue;
	}

	public void setRcvQueue(BlockingQueue<Message> rcvQueue) {
		this.rcvQueue = rcvQueue;
	}

	public BlockingQueue<Message> getSendDelayQueue() {
		return sendDelayQueue;
	}

	public void setSendDelayQueue(BlockingQueue<Message> sendDelayQueue) {
		this.sendDelayQueue = sendDelayQueue;
	}

	public BlockingQueue<Message> getRcvDelayQueue() {
		return rcvDelayQueue;
	}

	public void setRcvDelayQueue(BlockingQueue<Message> rcvDelayQueue) {
		this.rcvDelayQueue = rcvDelayQueue;
	}

	public HashMap<String, ObjectOutputStream> getConnectionCache() {
		return connectionCache;
	}

	public void setConnectionCache(HashMap<String, ObjectOutputStream> connectionCache) {
		this.connectionCache = connectionCache;
	}

	private class ServerThread implements Runnable {
		private MessagePasser mp;

		public ServerThread(MessagePasser mp) {
			this.mp = mp;
		}

		@Override
		public void run() {
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(mp.nodeMap.get(mp.name).getPort());
				while (true) {
					/* receiver is blocked -- blocking server implementation */
					Socket newConnection = null;
					try {
						newConnection = serverSock.accept();
						new WorkerThread(mp, newConnection);
					} catch (IOException e) {
						System.err.println("serverThread: Unable to accept new connection");
					}
				}
			} catch (IOException e) {
				System.err.println("serverThread: Unable to create server socket");
				System.exit(-1);
			} finally {
				try {
					if ( serverSock != null) {
						serverSock.close();
					}
				} catch (IOException e) {
					System.err.println("serverThread: Unable to close server socket");
					System.exit(-1);
				}
			}
		}
	}
}