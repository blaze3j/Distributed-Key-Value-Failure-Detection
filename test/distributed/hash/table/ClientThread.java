package distributed.hash.table;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;


/** 
 * Client thread that send request to a server for testing purpose 
 */
class ClientThread extends Thread {
    private IDistributedHashTable mDhtClient = null;
    private int mMinRange = 0;
    private int mMaxRange = 0;
    private int mMachineId = 0;
    private int mServerCount;
    private int[] mPortMap;
    
    /** 
     * Constructor
     */
    public ClientThread(int machineId, int minRange, int maxRange) throws Exception{
        
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
        
        mDhtClient = (IDistributedHashTable) 
            Naming.lookup("rmi://localhost:" + mPortMap[(machineId - 1)] + "/DistributedHashTable");
        mMinRange = minRange;
        mMaxRange = maxRange;
        mMachineId = machineId;
    }

    public void run() {
        for (int i = mMinRange; i < mMaxRange; i++)
        {
            IInsertRequest req = new InsertRequest(i, mMachineId, i, i);
            try {
                mDhtClient.insert(req);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }
}
