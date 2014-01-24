/**
 * File: CommunicationInfra.java
 * @author Ashish Kaila
 * @author Ying Li
 * @since  January 18th 2014
 *
 * Brief: Class used to represent Serializable object
 */
package lab0;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Message implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -4898787461414442556L;
	private String src = null;
	private String dest;
	private String kind;
	private Object data;
	private boolean isDuplicate = false;
	private int id = -1;

	public Message(String dest, String kind, Object data) {
		this.setDest(dest);
		this.setKind(kind);
		this.setData(data);
	}

	public Message duplicate() throws UnsupportedOperationException {
		Message msg = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;

		if (!(this.data instanceof java.io.Serializable)) {
			throw new UnsupportedOperationException();
		}
		try {
			bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			oos.writeObject(this);
			oos.flush();
			oos.close();

			bis = new ByteArrayInputStream(bos.toByteArray());
			ois = new ObjectInputStream(bis);
			msg = (Message)ois.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bos != null) {
					bos.close();
				}

				if (oos != null) {
					oos.close();
				}

				if (bis != null) {
					bis.close();
				}

				if (ois != null) {
					ois.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return msg;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getKind() {
		return kind;
	}

	public void setKind(String kind) {
		this.kind = kind;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public boolean isDuplicate() {
		return isDuplicate;
	}

	public void setDuplicate(boolean duplicate) {
		this.isDuplicate = duplicate;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
