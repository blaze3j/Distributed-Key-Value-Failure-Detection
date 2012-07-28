import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.Random;

import distributed.hash.table.IDistributedHashTable;
import distributed.hash.table.IInsertDeleteRequest;
import distributed.hash.table.InsertDeleteRequest;

public class DHTMovieLoader {
    protected static int mServerCount;
    protected static int[] mPortMap;
    protected static IDistributedHashTable[] mDhtClientArray = null;
    protected static int mRequestId = 1;
    protected static Random mRandom = null;

    public static void setUp() throws Exception {
        mRandom = new Random();
        
    	try{
            java.net.URL path = ClassLoader.getSystemResource("clientSetting4.txt");    
            FileReader fr = new FileReader (path.getFile());
            BufferedReader br = new BufferedReader (fr);
            try {
                String[] portMap = br.readLine().split(",");
                mServerCount = portMap.length;
                mPortMap = new int[mServerCount];
                for(int i = 0; i < mServerCount; i++){
                    mPortMap[i] = Integer.parseInt(portMap[i]);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        } catch (FileNotFoundException e2) {
            e2.printStackTrace();
            System.exit(-1);
        }
        
        mDhtClientArray = new IDistributedHashTable[mServerCount];
        for (int i = 0; i < mServerCount; i++) {
            mDhtClientArray[i] = (IDistributedHashTable) 
            Naming.lookup("rmi://localhost:" + mPortMap[i] + "/DistributedHashTable");
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        int count = 0;
       
        try {
            setUp();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        
        for (int i = 0; i < mServerCount; i++) {
            try {
                mDhtClientArray[i].purge();
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
        
        try{
            java.net.URL path = ClassLoader.getSystemResource("unique_movies_list.txt");
            FileReader fr = new FileReader (path.getFile());
            BufferedReader br = new BufferedReader (fr);
            String line;
            while ((line = br.readLine()) != null){
            	int machineClientId = mRandom.nextInt(mServerCount);
                int machineId = machineClientId + 1;
            	System.out.println("mRequestId: " + mRequestId + " machineId: " + machineId + " line: " + line);
                IInsertDeleteRequest req = new InsertDeleteRequest(mRequestId++, machineId, line, line);
                mDhtClientArray[machineClientId].insert(req);
    	    }
            
        } catch (FileNotFoundException e2) {
            e2.printStackTrace();
            System.exit(-1);
        } catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
        
        
        
        for (int i = 0; i < mServerCount; i++) {
            try {
                count =  mDhtClientArray[i].count();
                System.out.println("DHTServer[" + (i + 1) + "] count " + count);
                
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }

    }

}
