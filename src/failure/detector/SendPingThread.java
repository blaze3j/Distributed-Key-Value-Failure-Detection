package failure.detector;

import java.io.IOException;
import java.net.*;
import java.util.*;

public class SendPingThread extends Thread{
	private static final int MaxServersToPing = 2;
	private int myId;
	private LinkedHashMap<Integer, String> allPeers; // all servers that this server may ping
	private LinkedHashMap<Integer, String> pingPeers; // stores the current alive servers to ping
	private List<Integer> failedServer;
	Map.Entry<Integer, String> backupPeer; // the back up server if a live server failed
	FailureDetectorThread parentThread;
	
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
	
	// check if id is list as peer of this machine and is alive, otherwise return false as we don't know about it
	public boolean isAlive(Integer id){
		if(allPeers.containsKey(id))
			synchronized (failedServer) {
				return !this.failedServer.contains(id);	
			}
		return false;
	}

	public Map.Entry<Integer, String> getBackupPeer(){
		return backupPeer;
	}
	
	public void run() {
		while(true){
			for (Map.Entry<Integer, String> peer : pingPeers.entrySet()) {
				try{
					// TODO add host name to the address and parse it here instead of hard code localhost
					int peerId = peer.getKey();
					int peerPort = Integer.parseInt(peer.getValue());
					utils.Output.println("Send PING to server : " + peerId  + " in port: " + peerPort  + ".");
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
						utils.Output.println("error sending PING to server " + peerId + ": " + e.getMessage());
					} 
					
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					try{
						clientSocket.receive(receivePacket);
						String ack = new String(receivePacket.getData());
						utils.Output.println("ACK from server: " + ack);
					}
					catch(SocketTimeoutException timeout){
						synchronized(failedServer){
							utils.Output.println("Server " + peerId + " Failed.");
							failedServer.add(peerId);
						}
					}
					finally{
						if(clientSocket != null)
							clientSocket.close();
					}
				}catch (IOException e) {
					utils.Output.println("Send Thread: " + e.getMessage());
				}
			}
			// if there is at lease one failed server, try to reconstruct it. May be failed servers are back now
			if(failedServer.size() > 0)
				reconstructRing();
				
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				utils.Output.println("Send Thread: " + e.getMessage());
			}
		}
	}

	private void checkFailedServer(){
		List<Integer> liveServers = new ArrayList<Integer>();
		for (Integer id : failedServer) {
			try{
				int peerPort = Integer.parseInt(allPeers.get(id));
				utils.Output.println("Check server : " + id  + " in port: " + peerPort  + ".");
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
					utils.Output.println("error sending PING to server " + id + ": " + e.getMessage());
				} 
				
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				try{
					clientSocket.receive(receivePacket);
					utils.Output.println("Server : " + id  + " is joined back.");
					this.parentThread.fireServerJoin(id);
					liveServers.add(id);
				}catch(SocketTimeoutException e){ } 
				finally{
					if(clientSocket != null)
						clientSocket.close();
				}
			}catch (IOException e) { }
		}
		for(Integer id: liveServers){
			synchronized(failedServer){
				failedServer.remove(id);
			}
		}
	}
	
	private void reconstructRing() {
		utils.Output.println("Send Thread: reconstructRing");
		checkFailedServer(); // check the failed servers
		
		// remove failed server from the ring
		for (Integer id: this.failedServer) {
			if(pingPeers.containsKey(id)){
				utils.Output.println("server " + id + " is out of ring!");
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
				utils.Output.println("server " + id + " is in the ring!");
				pingPeers.put(id, peer.getValue());
				if(pingPeers.size() >= MaxServersToPing)
					return;
			}
		}
		
		// if 3 servers are failed, then exit the application
		if(pingPeers.isEmpty()){
			utils.Output.println("EXIT! at least 3 servers are failed!");
			System.exit(-1);
		}
	}
}
