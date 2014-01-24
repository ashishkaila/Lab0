/**
 * File: CommunicationInfra.java
 * @author Ashish Kaila
 * @author Ying Li
 * @since  January 18th 2014
 *
 * Brief: Class used to represent nodes in the DS environment
 */
package lab0;

public class Node {

	private String name;
	private String ip;
	private Integer port;

	public Node(String name, String ip, Integer port) {
		this.setName(name);
		this.setIp(ip);
		this.setPort(port);
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
