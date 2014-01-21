package lab0;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

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
		
		try {
			objIpStream = new ObjectInputStream(socket.getInputStream());
			while(true) {
				msg = (Message)objIpStream.readObject();
				System.err.println(msg.getData());
				assert msg instanceof Message;
				src = new String(msg.getSrc());
				mp.getRcvQueue().add(msg);
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
