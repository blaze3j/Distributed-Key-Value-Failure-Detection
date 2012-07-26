package distributed.hash.table;

import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConcurrencyExperiment2  extends TestExperiment {
    
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
     * Test method for 5x client threads doing 1000x insertions.
     */
    @Test
    public void testConcurrencyExperiment2() {
        final int clientThreadCount = 5;
        int count = 0;

        for (int i = 0; i < mServerCount; i++) {
            try {
                mDhtClientArray[i].purge();
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }

        ClientThreadExperiment2[] clientThreadArray = new ClientThreadExperiment2[clientThreadCount];
        
        for (int i = 0; i < clientThreadCount; i++) {
            try {
                clientThreadArray[i] = new ClientThreadExperiment2(i, i * 1000 + 1, (i + 1) * 1000 + 1);
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
        
        for (int i = 0; i < mServerCount; i++) {
            try {
                count =  mDhtClientArray[i].count();
                System.out.println("DHTServer[" + (i + 1) + "] count " + count);
                assertTrue(count != 0);
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }
    }
}
