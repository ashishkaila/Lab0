package lab0;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class CommunicationInfra {

	private static MessagePasser mp;

	public static void usage(int errCode) {
		switch (errCode) {
		case Constants.INVALID_USAGE:
		default:
			System.out.println("Usage: java CommunicationInfra <config file> <process uid> \n");
			break;
		case Constants.INVALID_CONFIG_FILE:
			System.out.println("Configuration file is invalid or incorrectly specified. \n");
			break;
		case Constants.INVALID_CONFIG_PARAMS:
			System.out.println("Configuration file has invalid parameters please revisit it. \n");
		}
		System.exit(-1);
	}

	public static void runApp() {
		String input;
		BufferedReader inputReader = null;

		inputReader = new BufferedReader(new InputStreamReader(System.in));
		while(true) {
			try {
				System.out.println("Enter: (1) (Ss)end <dest> <kind> <message> (2) (Rr)eceive (3) (Qq)uit");
				System.out.print("$>");
				input = inputReader.readLine();
				parseCommand(input);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}

	public static void parseCommand(String input) {
		if (input == null) {
			return;
		}

		String tokens[] = input.trim().split(" ");
		if (tokens[0].equalsIgnoreCase("S") && tokens.length >= 4) {
			/* Send case */
			if (!mp.nodeMap.containsKey(tokens[1])) {
				System.out.println("Invalid destination: " + tokens[1]);
				return;
			}

			StringBuilder msg = new StringBuilder();
			for (int i = 3; i < tokens.length; i++) {
				msg.append(tokens[i]);
				if (i >= 3 && tokens.length > 3) {
					msg.append(" ");
				}
			}

			mp.send(new Message(tokens[1], tokens[2], msg.toString()));
			return;
		} else if (tokens[0].equalsIgnoreCase("R") && tokens.length == 1) {
			System.out.println("Received: " + mp.receive().getData());
			return;
		} else if (tokens[0].equalsIgnoreCase("Q")) {
			System.out.println("Exiting...\n");
			System.exit(0);
		}
	}

	public static void main(String[] args) {
		/* The infrastructure requires 2 arguments */
		if (args.length != 2) {
			usage(Constants.INVALID_USAGE);
		}

		mp = new MessagePasser(args[0], args[1]);

		runApp();
	}
}
