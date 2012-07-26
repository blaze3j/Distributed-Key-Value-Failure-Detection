package distributed.hash.table;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * JUnit test to test distributed hash table functions 
 */
public class TestExperiment4 extends TestExperiment{
	
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
    	super.tearDown();
    }
	
    /**
     * Test method for validating 1000000 and collect overhead information 
     * on random lookups
     */
    @Test
    public void testExperiment4() {
        int total = 0;
        int count = 0;
        
        for (int i = 0; i < mServerCount; i++) {
            try {
                mDhtClientArray[i].purge();
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }

        for (int i = 1; i <= 1000000; i++)
        {
            try {
                int machineClientId = mRandom.nextInt(mServerCount);
                int machineId = machineClientId + 1;

                IInsertDeleteRequest req = new InsertDeleteRequest(mRequestId++, machineId, "" + i, i);
                mDhtClientArray[machineClientId].insert(req);
            }  catch(Exception e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
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
        
        assertTrue(total == 1000000);

        for (int i = 0; i < 1000; i++)
        {
            try {
                int machineClientId = mRandom.nextInt(mServerCount);
                int machineId = machineClientId + 1;
                int key = mRandom.nextInt(1000000) + 1;
                IQueryRequest req = new QueryRequest(mRequestId++, machineId, "" + key);
                int hops = mDhtClientArray[machineClientId].lookupTrace(req);
                System.out.println("DHTServer[" + machineId + "] hops " + hops);
            }  catch(RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
    }
}