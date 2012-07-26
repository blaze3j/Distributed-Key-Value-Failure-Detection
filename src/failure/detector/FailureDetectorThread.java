package failure.detector;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;


public class FailureDetectorThread extends Thread {
	private int serverId;
	private int serverPort;
	private LinkedHashMap<Integer, String> peers;
	private static FailureDetectorThread instance = null;
	private ReceivePingThread receiveThread = null;
	private SendPingThread sendThread = null;
	
	private transient Vector<ServerJoinListener> listeners;
	
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
		sendThread = new SendPingThread(this, serverId, peers);
	}
	
	/** Register a listener for ServerJoinEvent */
	synchronized public void addServerJoinListener(ServerJoinListener l) {
		if (listeners == null)
			listeners = new Vector<ServerJoinListener>();
		listeners.addElement(l);
	}
	  
	/** Remove a listener for ServerJoinEvent */
	synchronized public void removeServerJoinListener(ServerJoinListener l) {
		if (listeners == null)
			listeners = new Vector<ServerJoinListener>();
		listeners.removeElement(l);
	}
	
	/** Fire a ServerJoin to all registered listeners */
	protected void fireServerJoin(int serverId){
		// if we have no listeners, do nothing...
	    if (listeners != null && !listeners.isEmpty()) {
	    	// create the event object to send
	    	ServerJoinEvent event = new ServerJoinEvent(this, serverId);

	    	// make a copy of the listener list in case
	    	//   anyone adds/removes listeners
	    	Vector<ServerJoinListener> targets;
	    	synchronized (this){
	    		targets = (Vector<ServerJoinListener>) listeners.clone();
	    	}

	    	// walk through the listener list and
	    	//   call the sunMoved method in each
	    	Enumeration<ServerJoinListener> e = targets.elements();
	    	while (e.hasMoreElements()) {
	    		ServerJoinListener l = (ServerJoinListener) e.nextElement();
	    		l.onServerJoin(event);
	    	}
	    }
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
