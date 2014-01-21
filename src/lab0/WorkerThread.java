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
		Message dupMsg = null;
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
					this.mp.updateConfig();
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
							dupMsg = msg.duplicate();
							/* TODO -- should duplicate flag be set  */
							dupMsg.setDuplicate(true);
						} catch (UnsupportedOperationException e) {
							e.printStackTrace();
							System.err.println("Receiver failed to make a copy of message. \n");
						}
					}
				}
				
				this.mp.getRcvQueue().add(msg);
				if (dupMsg != null) {
					this.mp.getRcvQueue().add(dupMsg);
				}

				try {
					synchronized(this.mp) {
						while(!this.mp.getRcvDelayQueue().isEmpty()) {
							this.mp.getRcvQueue().add(this.mp.getRcvDelayQueue().remove());
						}
					}
				} catch (NoSuchElementException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (EOFException e) {
			System.err.println("connection terminated from " + src +
					" Posible Reason: " + e.getMessage());
		}
		 catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (objIpStream != null) {
				try {
					objIpStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
