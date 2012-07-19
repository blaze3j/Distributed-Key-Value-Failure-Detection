package distributed.hash.table;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * JUnit test to test insert request of distributed hash table 
 */
public class TestExperiment2 extends TestExperiment {

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
     * Test method for performing 1000 random insertions when system is empty.
     */
    @Test
    public void testExperiment2() {
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
                IInsertRequest req = new InsertRequest(mRequestId++, machineId, key, 1);

                mStopwatch.start(); 
                mDhtClientArray[machineClientId].insert(req);
                mStopwatch.stop();
                System.out.println("DHTServer[" + machineId + "] insert on empty took " + mStopwatch.getElapsedTime());

                mDhtClientArray[machineClientId].purge();
                assertTrue(0 == mDhtClientArray[machineClientId].count());
            }  catch(RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
    }
}