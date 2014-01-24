/**
 * File: CommunicationInfra.java
 * @author Ashish Kaila
 * @author Ying Li
 * @since  January 18th 2014
 *
 * Brief: Thread functionality that accepts connections
 */
package lab0;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.NoSuchElementException;

public class WorkerThread implements Runnable {

	private MessagePasser mp;
	private Socket socket;

	public WorkerThread(MessagePasser mp, Socket socket) {
		this.mp = mp;
		this.socket = socket;
		new Thread(this).start();
	}

	@Override
	public void run() {
		ObjectInputStream objIpStream = null;
		Message msg = null;
		String src = null;
		Rule msgRule = null;

		try {
			objIpStream = new ObjectInputStream(socket.getInputStream());
			while(true) {
				msg = (Message)objIpStream.readObject();
				assert msg instanceof Message;
				src = new String(msg.getSrc());

				/* update the rules if configuration file was changed */
				synchronized(this.mp) {
					msgRule = this.mp.getRcvRule(msg);
				}

				if (msgRule != null) {
					if (msgRule.getAction().equals("drop")) {
						continue;
					} else if (msgRule.getAction().equals("delay")) {
						/* add message to delay queue */
						synchronized(this.mp) {
							this.mp.getRcvDelayQueue().add(msg);
						}
						continue;
					} else if (msgRule.getAction().equals("duplicate")) {
						try {
							Message dupMsg = msg.duplicate();
							dupMsg.setDuplicate(true);
							if (dupMsg != null) {
								this.mp.getRcvQueue().add(dupMsg);
							}
						} catch (UnsupportedOperationException e) {
							System.err.println("Receiver failed to make a copy of message. \n");
						}
					}
				}

				this.mp.getRcvQueue().add(msg);

				try {
					synchronized(this.mp) {
						while(!this.mp.getRcvDelayQueue().isEmpty()) {
							this.mp.getRcvQueue().add(this.mp.getRcvDelayQueue().remove());
						}
					}
				} catch (NoSuchElementException e) {
					System.err.println("workerThread: No more messages in delay queue");
				} catch (Exception e) {
					System.err.println("workerThread: Generic exception");
				}
			}
		} catch (ClassNotFoundException e) {
			System.err.println("workerThread: Unexpected class object sent over the wire");
		} catch (EOFException e) {
			System.err.println("connection terminated from " + src);
		} catch (IOException e) {
			System.err.println("workerThread: Unexpected I/O error encountered");
		} finally {
			if (objIpStream != null) {
				try {
					objIpStream.close();
				} catch (IOException e) {
					System.err.println("workerThread: Exception while trying to close stream");
				}
			}
		}
	}
}
