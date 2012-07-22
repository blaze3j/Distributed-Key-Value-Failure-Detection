import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.sun.org.apache.xalan.internal.xsltc.cmdline.getopt.GetOpt;

import failure.detector.*;


public class FailureDetector {

	private static int serverId;
	private static int serverPort;
	private static int mServerCount;
	private static int[] mPortMap;
	private static LinkedHashMap<Integer, Integer> peers;
	
	public static void start(int id, String failureSettingFile){
		
		try{
			// read server setting file
			java.net.URL path = ClassLoader.getSystemResource(failureSettingFile);	
			FileReader fr = new FileReader(failureSettingFile);//path.getFile());
	        BufferedReader br = new BufferedReader (fr);
	        String line;
	        try {
	        	peers = new LinkedHashMap<Integer, Integer>();
	        	while ((line = br.readLine()) != null){
					String[] serverSetting = line.split(",");
					int i = 0;
					if(Integer.parseInt(serverSetting[i]) == serverId){
						serverPort = Integer.parseInt(serverSetting[++i]);
						peers.put(Integer.parseInt(serverSetting[++i]), Integer.parseInt(serverSetting[++i]));
						peers.put(Integer.parseInt(serverSetting[++i]), Integer.parseInt(serverSetting[++i]));
						peers.put(Integer.parseInt(serverSetting[++i]), Integer.parseInt(serverSetting[++i]));
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
			System.exit(-1);
		}
		
		ReceivePingThread receiveThread = new ReceivePingThread(serverId, serverPort, null);
		SendPingThread sendThread = new SendPingThread(serverId, peers);
		
		receiveThread.start();
		sendThread.start();
		
		try {
			receiveThread.join();
			sendThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String failureSettingFile = "";
		GetOpt getopt = new GetOpt(args, "i:f:");
		serverId = 1;
		try {
			int c;
			while ((c = getopt.getNextOption()) != -1) {
			    switch(c) {
			    case 'i':
			    	serverId = Integer.parseInt(getopt.getOptionArg());
			        break;
			    case 'f':
			    	failureSettingFile = getopt.getOptionArg();
			        break;
			    }
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		start(serverId, failureSettingFile);
	}

}
