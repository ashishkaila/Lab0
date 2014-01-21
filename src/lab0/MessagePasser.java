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
	private int generateSendRules() {
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
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					return -1;
				}
			}
		}
		
		/* if parsing the configuration worked update send rules */
		this.sendRules = sendRules;
		return 0;
	}
	
	@SuppressWarnings("unchecked")
	private int generateRcvRules() {
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
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					e.printStackTrace();
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
			e.printStackTrace();
			CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
			return -1;
		} finally {
			if (configFileStream != null) {
				try {
					configFileStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					CommunicationInfra.usage(Constants.INVALID_CONFIG_FILE);
					return -1;
				}
			}
		}
		
		return 0;
	}

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

	public void send(Message message) {
		long modifiedTime = 0;
		Rule msgRule = null;
		Message dupMsg = null;
		boolean sendDelay = false;
		
		if (message == null || message.getDest() == null) {
			return;
		}
		
		/* TODO - Are we allowed to send to self */
		if (nodeMap.get(message.getDest()) == null) {
			System.err.println("Tried sending message to unknown host " + message.getDest());
			return;
		}
		
		message.setId(id.incrementAndGet());
		message.setSrc(this.name);
		
		/* reload rules if configuration file has been modified */
		modifiedTime = this.config.lastModified();
		
		synchronized(this) {
			if (this.configLastModifiedTime < modifiedTime) {
				generateSendRules();
				generateRcvRules();
				this.configLastModifiedTime = modifiedTime;
				}
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
						this.sendDelayQueue.add(message);
					return;
				} else if (msgRule.getAction().equals("duplicate")) {
					/* create a duplicate of the message */
					dupMsg = message.duplicate();
					dupMsg.setDuplicate(true);
				}
			}
			
			/* now send the message(s) */
			actuallySend(message);
			sendDelay = true;
		} catch (UnsupportedOperationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			/* it's possible that the sendQueue became empty but we still didn't
			 * send the duplicate message, in that case we need to check that 
			 */
			if (dupMsg != null) {
				actuallySend(dupMsg);
			}
			
			if (sendDelay) {
				try {
					synchronized(this) {
						while (!this.sendDelayQueue.isEmpty()) {
							actuallySend(sendDelayQueue.remove());
						}
					}
				} catch (NoSuchElementException e){
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
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
			e.printStackTrace();
			System.err.println("Unable to connect to host " + message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}
				/* TODO -- does closing the stream ensure underlying socket is also closed */
				if (tmpStream != null) {
					tmpStream.close();
				}
			} catch (Exception ex){
				ex.printStackTrace();
			}
		} catch (SocketException e) {
			e.printStackTrace();
			System.err.println("Unable to create or access Socket to destination "
			+ message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}
				/* TODO -- does closing the stream ensure underlying socket is also closed */
				if (tmpStream != null) {
					tmpStream.close();
				}
			} catch (Exception ex){
				ex.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("An I/O exception occurred while sending to destination "
			+ message.getDest());
			try {
				ObjectOutputStream tmpStream = null;
				synchronized(this) {
					 tmpStream = this.getConnectionCache().remove(message.getDest());
				}
				/* TODO -- does closing the stream ensure underlying socket is also closed */
				if (tmpStream != null) {
					tmpStream.close();
				}
			} catch (Exception ex){
				ex.printStackTrace();
				System.err.println("An unknown exception occurred while sending to destination "
						+ message.getDest());
				try {
					ObjectOutputStream tmpStream = null;
					synchronized(this) {
						 tmpStream = this.getConnectionCache().remove(message.getDest());
					}
					/* TODO -- does closing the stream ensure underlying socket is also closed */
					if (tmpStream != null) {
						tmpStream.close();
					}
				} catch (Exception exc){
					exc.printStackTrace();
				}
			}
		} 
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
					Socket newConnection = serverSock.accept();
					new WorkerThread(mp, newConnection);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if ( serverSock != null) {
						serverSock.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}