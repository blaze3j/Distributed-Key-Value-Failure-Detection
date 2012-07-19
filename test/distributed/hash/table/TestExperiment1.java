package distributed.hash.table;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * JUnit test to test lookup request of distributed hash table 
 */
public class TestExperiment1 extends TestExperiment {
	
    /**
     * @throws java.lang.Exception
     */
    @Before
	public void setUp() throws Exception {
    	super.setUp();
	}
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    	super.setUp();
    }
    
    /**
     * Test method for performing 1000 random key lookups when system is empty.
     */
    @Test
    public void testExperiement1() {
        for (int i = 0; i < mServerCount; i++) {
            try {
                mDhtClientArray[i].purge();
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
        mStopwatch.reset();

        for (int i = 0; i < 1000; i++)
        {
            try {
                int machineClientId = mRandom.nextInt(mServerCount);
                int machineId = machineClientId + 1;
                int key = mRandom.nextInt(1000000) + 1;
                IQueryRequest req = new QueryRequest(mRequestId++, machineId, key);

                mStopwatch.start(); 
                mDhtClientArray[machineClientId].lookup(req);
                mStopwatch.stop();
                System.out.println("DHTServer[" + machineId + "] lookup on empty took " + mStopwatch.getElapsedTime());

                assertTrue(0 == mDhtClientArray[machineClientId].count());
            }  catch(RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
    }
}