import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.sun.org.apache.xalan.internal.xsltc.cmdline.getopt.GetOpt;

import failure.detector.ReceiveHeartBeatThread;
import failure.detector.SendHeartBeatThread;


public class FailureDetector {

	private static int serverId;
	private static int serverPort;
	private static int mServerCount;
	private static int[] mPortMap;
	private static LinkedHashMap<Integer, Integer> peers;
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
		
		
		try{
			// read server setting file
			java.net.URL path = ClassLoader.getSystemResource(failureSettingFile);	
			FileReader fr = new FileReader (failureSettingFile);//path.getFile());
	        BufferedReader br = new BufferedReader (fr);
	        String line;
	        try {
	        	peers = new LinkedHashMap<Integer, Integer>();
				while ((line = br.readLine()) != null){
					String[] serverSetting = line.split(",");				
					if(Integer.parseInt(serverSetting[0]) == serverId){
						serverPort = Integer.parseInt(serverSetting[1]);
						int count = serverSetting.length;
						for(int i = 2 ; i < count;){
							peers.put(Integer.parseInt(serverSetting[i]), Integer.parseInt(serverSetting[i+1]));
							i+=2;
						}
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
		
		ReceiveHeartBeatThread receiveThread = new ReceiveHeartBeatThread(serverId, serverPort, null);
		SendHeartBeatThread sendThread = new SendHeartBeatThread(serverId, peers);
		
		receiveThread.start();
		sendThread.start();
		
		try {
			receiveThread.join();
			sendThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
