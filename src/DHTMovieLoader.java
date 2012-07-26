import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.Random;

import Stopwatch.Stopwatch;
import distributed.hash.table.IDistributedHashTable;
import distributed.hash.table.IInsertRequest;
import distributed.hash.table.InsertRequest;


public class DHTMovieLoader {
    protected static int mServerCount;
    protected static int[] mPortMap;
    protected static IDistributedHashTable[] mDhtClientArray = null;
    protected static int mRequestId = 1;
    protected static Random mRandom = null;
    protected static LinkedList<String> mLinkedList = null;

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
        
        mLinkedList = new LinkedList<String>();
        try{
            java.net.URL path = ClassLoader.getSystemResource("movies.list");
            FileReader fr = new FileReader (path.getFile());
            BufferedReader br = new BufferedReader (fr);

            for (int i = 0; i < 10; i++) {
                System.out.println(br.readLine());
            }
            
        } catch (FileNotFoundException e2) {
            e2.printStackTrace();
            System.exit(-1);
        }

        System.exit(0);
        
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
        int total = 0;
        
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

        
        
        try {
            int machineClientId = mRandom.nextInt(mServerCount);
            int machineId = machineClientId + 1;
            int key = mRandom.nextInt(1000000) + 1;
            IInsertRequest req = new InsertRequest(mRequestId++, machineId, "" + key, 1);

            mDhtClientArray[machineClientId].insert(req);
            
        }  catch(RemoteException e) {
            e.printStackTrace();
            System.out.println("dhtClient: " +  e.getMessage());
        }
        
        for (int i = 0; i < mServerCount; i++) {
            try {
                count =  mDhtClientArray[i].count();
                total += count;
                System.out.println("DHTServer[" + (i + 1) + "] count " + count);
                
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }

    }

}
