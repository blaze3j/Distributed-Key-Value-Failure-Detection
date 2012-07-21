package failure.detector;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;

public class ReceiveHeartBeatThread extends Thread{
	
	private int myId;
	private int myPort;
	private List<Integer> myPeers;
	
	
	public ReceiveHeartBeatThread(int id, int port, List<Integer> peers){
		this.myId = id;
		this.myPort = port;
		this.myPeers = peers;
	}
	public void run(){
        DatagramSocket serverSocket = null;
		try {
			serverSocket = new DatagramSocket(this.myPort);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        while(true){
        	byte[] receiveData = new byte[1024];
        	DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        	try {
        		serverSocket.receive(receivePacket);
        		String sentence = new String( receivePacket.getData());
        		System.out.println("RECEIVED: " + sentence);
        		InetAddress IPAddress = receivePacket.getAddress();
        		int port = receivePacket.getPort();
        		String ackData = "Ack " + this.myId;
        		DatagramPacket sendPacket =new DatagramPacket(ackData.getBytes(), ackData.length(), IPAddress, port);
        		serverSocket.setSoTimeout(1);
        		try{
        			// ignore client timeout
        			serverSocket.send(sendPacket);
            	} catch (IOException e) {
            		System.out.println("receive hearbeat 1");
            		// e.printStackTrace();
            	}
        	} catch (IOException e) {
        		// System.out.println("receive hearbeat 2");
        		// e.printStackTrace();
        	}
       }
	}
}
