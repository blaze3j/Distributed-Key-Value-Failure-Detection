import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.LinkedHashMap;

import com.sun.org.apache.xalan.internal.xsltc.cmdline.getopt.GetOpt;

import distributed.hash.table.DistributedHashTable;

/** 
 * the main application to initialize a data hash table on a server
 */
public class DHTServer {
	private static int serverId;
	private static String  serverSettingFile;
	private static int serverPort;
	private static int startKey;
	private static int serverSize;
	private static LinkedHashMap<String, Integer> successor;
	public static void main(String[] args) {

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
			    	serverSettingFile = getopt.getOptionArg();
			        break;
			    }
			    
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		try{
			// read server setting file
			java.net.URL path = ClassLoader.getSystemResource(serverSettingFile);	
			FileReader fr = new FileReader (path.getFile());
	        BufferedReader br = new BufferedReader (fr);
	        String line;
	        try {
	        	successor = new LinkedHashMap<String, Integer>();
				while ((line = br.readLine()) != null){
					String[] serverSetting = line.split(",");				
					if(Integer.parseInt(serverSetting[0]) == serverId){
						serverPort = Integer.parseInt(serverSetting[1]);
						startKey = Integer.parseInt(serverSetting[2]);
						serverSize = Integer.parseInt(serverSetting[3]);
						int count = serverSetting.length;
						for(int i = 4 ; i < count;){
							successor.put(serverSetting[i], Integer.parseInt(serverSetting[i+1]));
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
		
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new RMISecurityManager());
            System.out.println("Security manager installed.");
        } else {
            System.out.println("Security manager already exists.");
        }
		try { //special exception handler for registry creation
			LocateRegistry.createRegistry(serverPort); 
			System.out.println("java RMI registry created on port: " + serverPort);
		} catch (RemoteException e) {
		    //do nothing, error means registry already exists
			System.out.println("java RMI registry already exists.");
		}

		try{
			// initialize the server for this process
			DistributedHashTable dhtServer = new DistributedHashTable(serverId, startKey, serverSize, successor);
			Naming.rebind("//localhost:"+serverPort+"/DistributedHashTable", dhtServer);
            System.out.println("Distributed Hash server on machine: " + serverId + " is running.");
		}catch(RemoteException e){
			System.out.println("dhtServer: " + e.getMessage());
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
