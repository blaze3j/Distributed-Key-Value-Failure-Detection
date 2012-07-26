package failure.detector;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SendHeartBeatThread extends Thread{
	private int myId;
	private LinkedHashMap<Integer, Integer> peers;
	
	public SendHeartBeatThread(int id, LinkedHashMap<Integer, Integer> heartBeatPeers){
		this.myId = id;
		peers = heartBeatPeers;	
	}
	
	public void run() {
		while(true){
			for (Map.Entry<Integer, Integer> peer : peers.entrySet()) {
				try {
					// TODO add host name to the address and parse it here instead of hard code localhost
					int peerId = peer.getKey();
					int peerPport = peer.getValue();
					System.out.println("send hearbeat to peer : " + peerId  + " in port: " + peerPport  + ".");
					String heartBeat = "Heartbeat from " + this.myId;
					DatagramSocket clientSocket = new DatagramSocket();
					clientSocket.setSoTimeout(1000);
					InetAddress IPAddress = InetAddress.getByName("localhost");
					byte[] sendData = new byte[1024];
					byte[] receiveData = new byte[1024];
					sendData = heartBeat.getBytes();
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, peerPport );
					clientSocket.send(sendPacket);
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					clientSocket.receive(receivePacket);
					String ack = new String(receivePacket.getData());
					System.out.println("FROM SERVER:" + ack);
					clientSocket.close();
				} catch (SocketException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
