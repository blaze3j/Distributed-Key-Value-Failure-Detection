package failure.detector;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SendPingThread extends Thread{
	private int myId;
	private LinkedHashMap<Integer, Integer> peers;
	
	public SendPingThread(int id, LinkedHashMap<Integer, Integer> pingPeers){
		this.myId = id;
		peers = pingPeers;	
	}
	
	public void run() {
		while(true){
			for (Map.Entry<Integer, Integer> peer : peers.entrySet()) {
				try{
					// TODO add host name to the address and parse it here instead of hard code localhost
					int peerId = peer.getKey();
					int peerPort = peer.getValue();
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
						utils.Output.println("Server " + peerId + " Failed.");
					}
					clientSocket.close();
				}catch (IOException e) {
					utils.Output.println("Send Thread: " + e.getMessage());
				}
			}
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				utils.Output.println("Send Thread: " + e.getMessage());
			}
		}
	}
}
