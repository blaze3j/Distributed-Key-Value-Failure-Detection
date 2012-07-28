package failure.detector;

import java.io.IOException;
import java.net.*;
import java.util.*;

/** 
 * creates a send ping thread and manages failure and back up servers
 * Exit system if there are at least 3 serves failure 
 */
public class SendPingThread extends Thread{
	private static final int MaxServersToPing = 2;
	private int myId;
	private LinkedHashMap<Integer, String> allPeers; // all servers that this server may ping
	private LinkedHashMap<Integer, String> pingPeers; // stores the current alive servers to ping
	private List<Integer> failedServer;
	Map.Entry<Integer, String> backupPeer; // the back up server if a live server failed
	FailureDetectorThread parentThread;

	/**
	* constructor
    */
	public SendPingThread(FailureDetectorThread parent, int id, LinkedHashMap<Integer, String> peers){
		this.parentThread = parent;
		this.myId = id;
		this.allPeers = peers;
		pingPeers = new LinkedHashMap<Integer, String>();
		int i = 1;
		for (Map.Entry<Integer, String> peer : peers.entrySet()){
			if(i++ <= MaxServersToPing)
				pingPeers.put(peer.getKey(), peer.getValue());
			else
				backupPeer = peer;
		}
		this.failedServer = new ArrayList<Integer>();
	}

	/**
	* returns the back up server 
    */
	public Map.Entry<Integer, String> getBackupPeer(){
		return backupPeer;
	}
	
	/**
	* starts the send ping thread
    */
	public void run() {
		while(true){
			for (Map.Entry<Integer, String> peer : pingPeers.entrySet()) {
				try{
					// TODO add host name to the address and parse it here instead of hard code localhost
					int peerId = peer.getKey();
					int peerPort = Integer.parseInt(peer.getValue());
					FailureDetectorThread.handleMessage("Send PING to server : " + peerId  + " in port: " + peerPort  + ".");
					String ping = "PING from " + this.myId;
					DatagramSocket clientSocket = new DatagramSocket();
					// If no ACK after 1 second, then report failure 
					clientSocket.setSoTimeout(1000);
					InetAddress IPAddress = InetAddress.getByName("localhost");
					byte[] sendData = new byte[1024];
					byte[] receiveData = new byte[1024];
					sendData = ping.getBytes();
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, peerPort );
					
					try{
						clientSocket.send(sendPacket);
					}catch (SocketException e) {
						FailureDetectorThread.handleMessage("error sending PING to server " + peerId + ": " + e.getMessage());
					} 
					
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					try{
						clientSocket.receive(receivePacket);
						String ack = new String(receivePacket.getData());
						FailureDetectorThread.handleMessage("ACK from server: " + ack);
					}
					catch(SocketTimeoutException timeout){
						synchronized(failedServer){
							FailureDetectorThread.handleMessage("Server " + peerId + " Failed.");
							failedServer.add(peerId);
						}
					}
					finally{
						if(clientSocket != null)
							clientSocket.close();
					}
				}catch (IOException e) {
					FailureDetectorThread.handleMessage("Send Thread: " + e.getMessage());
				}
			}
			// if there is at lease one failed server, try to reconstruct it. May be failed servers are back now
			if(failedServer.size() > 0){
				try {
					sleep(1000);
				} catch (InterruptedException e) {
					FailureDetectorThread.handleMessage("Send Thread: " + e.getMessage());
				}
				reconstructRing();
			}
				
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				FailureDetectorThread.handleMessage("Send Thread: " + e.getMessage());
			}
		}
	}

	
	/**
	* check if id is list as peer of this machine and is alive, 
	* otherwise return false
    */
	public boolean isServerAlive(int id){ 
		synchronized(failedServer){
			if(failedServer.contains(id))
					return false;
		}
		return isAlive(id);
	}
	/**
	* check if id is list as peer of this machine and is alive, 
	* otherwise return false
    */
	private boolean isAlive(int id){
		try{
			int peerPort = Integer.parseInt(allPeers.get(id));
			FailureDetectorThread.handleMessage("Check server : " + id  + " in port: " + peerPort  + ".");
			String ping = "PING from " + this.myId;
			DatagramSocket clientSocket = new DatagramSocket(); 
			clientSocket.setSoTimeout(1000);
			InetAddress IPAddress = InetAddress.getByName("localhost");
			byte[] sendData = new byte[1024];
			byte[] receiveData = new byte[1024];
			sendData = ping.getBytes();
			DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, peerPort );
			try{
				clientSocket.send(sendPacket);
			}catch (SocketException e) {
				FailureDetectorThread.handleMessage("error sending PING to server " + id + ": " + e.getMessage());
			} 
			
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			try{
				clientSocket.receive(receivePacket);
				FailureDetectorThread.handleMessage("Server : " + id  + " is alive.");
				return true;
			}catch(SocketTimeoutException e){ } 
			finally{
				if(clientSocket != null)
					clientSocket.close();
			}
		}catch (IOException e) { }
		return false;

	}
	
	/**
	* checks and update failed servers
    */
	private void checkFailedServer(){
		List<Integer> liveServers = new ArrayList<Integer>();
		for (Integer id : failedServer) {
			if(isAlive(id))
			{
				liveServers.add(id);
				this.parentThread.fireServerJoin(id);
			}
		}
		for(Integer id: liveServers){
			synchronized(failedServer){
				failedServer.remove(id);
			}
		}
	}
	
	/**
	* constructor
    */
	private void reconstructRing() {
		FailureDetectorThread.handleMessage("Send Thread: reconstructRing");
		checkFailedServer(); // check the failed servers
		
		// remove failed server from the ring
		for (Integer id: this.failedServer) {
			if(pingPeers.containsKey(id)){
				FailureDetectorThread.handleMessage("server " + id + " is out of ring!");
				pingPeers.remove(id);
			}
		}
		
		// there is no failed server, reconstruct normal ring
		if(failedServer.size() == 0){
			pingPeers.clear();
			int i = 1;
			for (Map.Entry<Integer, String> peer : allPeers.entrySet()){
				if(i++ <= MaxServersToPing)
					pingPeers.put(peer.getKey(), peer.getValue());
			}
			return;
		}
		
		// there is at lease one failure, reconstruct the ring if there is any live server left
		for (Map.Entry<Integer, String> peer : allPeers.entrySet()) {
			int id = peer.getKey();
			if(failedServer.contains(id))
				continue;
			else{
				FailureDetectorThread.handleMessage("server " + id + " is in the ring!");
				pingPeers.put(id, peer.getValue());
				if(pingPeers.size() >= MaxServersToPing)
					return;
			}
		}
		
		// if 3 servers are failed, then exit the application
		if(pingPeers.isEmpty()){
			FailureDetectorThread.handleMessage("EXIT! at least 3 servers are failed!");
			System.exit(-1);
		}
	}
}
