package distributed.hash.table;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.util.Random;

import org.junit.After;

import Stopwatch.Stopwatch;

/** 
 * base class to set up clients for distributed hash table 
 */
public abstract class TestExperiment {
	
	protected int mServerCount;
	protected int[] mPortMap;
	protected IDistributedHashTable[] mDhtClientArray = null;
    protected int mRequestId = 1;
    protected Random mRandom = null;
    protected Stopwatch mStopwatch = null;
	
	public void setUp() throws Exception {
        mRandom = new Random();
        mStopwatch = new Stopwatch();		
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
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }
}
