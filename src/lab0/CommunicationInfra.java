package lab0;

public class CommunicationInfra {

	private MessagePasser mp;

	public CommunicationInfra(MessagePasser mp) {
		this.setMessagePasser(mp);
	}

	public static void usage(int errCode) {
		switch (errCode) {
		case Constants.INVALID_USAGE:
		default:
			System.out.println("Usage: java CommunicationInfra <config file> <process name> \n");
			break;
		case Constants.INVALID_CONFIG_FILE:
			System.out.println("Configuration file is invalid or incorrectly specified. \n");
			break;
		case Constants.INVALID_CONFIG_PARAMS:
			System.out.println("Configuration file has invalid parameters please revisit it. \n");
		}
		System.exit(-1);
	}

	public MessagePasser getMessagePasser() {
		return mp;
	}

	public void setMessagePasser(MessagePasser mp) {
		this.mp = mp;
	}

	public static void main(String[] args) {

		/* The infrastructure requires 2 arguments */
		if (args.length != 2) {
			usage(Constants.INVALID_USAGE);
		}

		MessagePasser mp = new MessagePasser(args[0], args[1]);
		CommunicationInfra shim = new CommunicationInfra(mp);
		AppThread appThread = new AppThread(shim);
		new Thread(appThread).start();
	}
}