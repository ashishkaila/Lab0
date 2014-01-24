/**
 * File: CommunicationInfra.java
 * @author Ashish Kaila
 * @author Ying Li
 * @since  January 18th 2014
 *
 * Brief: Class used to maintain sending/receiving rules
 */
package lab0;

public class Rule {
	private String action = null;
	private String src = null;
	private String dest = null;
	private String kind = null;
	private Integer seqnum = null;

	public boolean matches(Message msg) {

		/* if only action is in the rule it will match all messages */
		if (src == null && dest == null && kind == null && seqnum == null) {
			return true;
		}

		if (src != null && !src.equals(msg.getSrc())) {
			return false;
		}

		if (dest != null && !dest.equals(msg.getDest())) {
			return false;
		}

		if (kind != null && !kind.equals(msg.getKind())) {
			return false;
		}

		if (seqnum != null && !seqnum.equals(msg.getId())) {
			return false;
		}

		return true;
	}

	public Rule(String action) {
		this.action = action;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
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

	public Integer getSeqnum() {
		return seqnum;
	}

	public void setSeqnum(Integer seqnum) {
		this.seqnum = seqnum;
	}

}
