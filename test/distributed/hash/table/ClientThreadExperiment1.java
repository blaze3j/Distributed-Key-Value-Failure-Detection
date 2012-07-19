package distributed.hash.table;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Random;

import Stopwatch.Stopwatch;

import distributed.hash.table.IDistributedHashTable;

/** 
 *  Helper class for spawning threads for lookup on empty
 */
class ClientThreadExperiment1 extends Thread {
    protected IDistributedHashTable[] mDhtClientArray = null;
    private int mMinRange = 0;
    private int mMaxRange = 0;
    private int mServerCount = 0;
    private int mIdentifier = 0;
    private int[] mPortMap;
    private Random mRandom = null;
    private Stopwatch mStopwatch = null;
    
    public ClientThreadExperiment1(int identifier, int minRange, int maxRange) throws Exception{
        
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

        mMinRange = minRange;
        mMaxRange = maxRange;
        mIdentifier = identifier;
        
        mRandom = new Random();
        mStopwatch = new Stopwatch();
    }

    public void run() {
        mStopwatch.reset();

        for (int i = mMinRange; i < mMaxRange; i++)
        {
            int machineClientId = mRandom.nextInt(mServerCount);
            int machineId = machineClientId + 1;
            int key = mRandom.nextInt(1000000) + 1;
            IQueryRequest req = new QueryRequest(i, machineId, key);
            try {
                mStopwatch.start(); 
                mDhtClientArray[machineClientId].lookup(req);
                mStopwatch.stop();
                System.out.println("Thread[" + mIdentifier + "] DHTServer[" + machineId + "] lookup on empty took " + mStopwatch.getElapsedTime());
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }
}
