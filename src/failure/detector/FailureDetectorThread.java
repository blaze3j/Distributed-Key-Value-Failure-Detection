package failure.detector;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;


public class FailureDetectorThread extends Thread {
	private int serverId;
	private int serverPort;
	private LinkedHashMap<Integer, String> peers;
	private static FailureDetectorThread instance = null;
	ReceivePingThread receiveThread = null;
	SendPingThread sendThread = null;
	
	public static FailureDetectorThread getInstance(int id, String failureSettingFile) throws NumberFormatException, IOException{
		if(instance == null)
			instance = new FailureDetectorThread(id, failureSettingFile);
		return instance;
	}
	
	public static boolean isAlive(Integer id){
		return instance.sendThread.isAlive(id);
	}
	
	public static  Map.Entry<Integer, String> getBackupPeer(){
		return instance.sendThread.backupPeer;
	}
		
	private FailureDetectorThread(int id, String failureSettingFile) throws NumberFormatException, IOException {
	    System.out.println("Recieved id " + id + " settings file " + failureSettingFile);
	    // read server setting file
	    java.net.URL path = ClassLoader.getSystemResource(failureSettingFile);	
	    FileReader fr = new FileReader(path.getFile());
	    BufferedReader br = new BufferedReader (fr);
	    String line;
	    serverId = id;
	    
	    peers = new LinkedHashMap<Integer, String>();
	    while ((line = br.readLine()) != null){
	        String[] serverSetting = line.split(",");
	        int i = 0;
	        if(Integer.parseInt(serverSetting[i]) == serverId){
	            serverPort = Integer.parseInt(serverSetting[++i]);
	            peers.put(Integer.parseInt(serverSetting[++i]), serverSetting[++i]);
	            peers.put(Integer.parseInt(serverSetting[++i]), serverSetting[++i]);
	            peers.put(Integer.parseInt(serverSetting[++i]), serverSetting[++i]);
	            break;
	        }
	    }
	    
	    receiveThread = new ReceivePingThread(serverId, serverPort, null);
		sendThread = new SendPingThread(serverId, peers);
	}
	
	public void run() {
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
