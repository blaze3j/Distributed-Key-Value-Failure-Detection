package distributed.hash.table;

import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * Testing the concurrent request to multi servers
 */
public class TestConcurrency extends TestExperiment{

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
     * Test method for {@link distributed.hash.table.Request#Request(int, int, int, java.lang.Object)}.
     */
    @Test
    public void testConcurrency1() {
        final int clientThreadCount = 5;
        final int machineId = 1;
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

        ClientThread[] clientThreadArray = new ClientThread[clientThreadCount];
        
        // create and run client threads
        for (int i = 0; i < clientThreadCount; i++) {
            try {
                clientThreadArray[i] = new ClientThread(machineId, i * 1000 + 1, (i + 1) * 1000 + 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            clientThreadArray[i].start();
        }
        
        for (int i = 0; i < clientThreadCount; i++) {
            try {
                clientThreadArray[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        // client threads are done, get the count on each server
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
        
        assertTrue(total == 1000 * clientThreadCount);
    }
}
