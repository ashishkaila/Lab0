package lab0;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public  class AppThread implements Runnable {
	private CommunicationInfra shim;

	public AppThread() {

	}
	public AppThread(CommunicationInfra shim) {
		this.shim = shim;
	}

	@Override
	public void run() {
		runApp();
	}

	public void runApp() {
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

	private void parseCommand(String input) {
		if (input == null) {
			return;
		}

		String tokens[] = input.trim().split(" ");
		if (tokens[0].equalsIgnoreCase("S") && tokens.length >= 4) {
			/* Send case */
			if (!this.shim.getMessagePasser().nodeMap.containsKey(tokens[1])) {
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

			this.shim.getMessagePasser().send(new Message(tokens[1], tokens[2], msg.toString()));
			return;
		} else if (tokens[0].equalsIgnoreCase("R") && tokens.length == 1) {
			Message msg = this.shim.getMessagePasser().receive();
			System.out.println("Received: " +
					msg.getData() + " (kind:) " +
					msg.getKind() + " (src:) " +
					msg.getSrc() + " (id:) " +
					msg.getId() + " (is_duplicate:) " +
					msg.isDuplicate());
			return;
		} else if (tokens[0].equalsIgnoreCase("Q")) {
			System.out.println("Exiting...\n");
			System.exit(0);
		}
	}
}