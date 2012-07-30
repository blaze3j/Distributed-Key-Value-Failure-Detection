package distributed.hash.table;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestExperiment5  extends TestExperiment {

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
    
	@Test
	public void testExperimentMovieLoader() {
        int count = 0;
        int total = 0;
        
		for (int i = 0; i < mServerCount; i++) {
            try {
                mDhtClientArray[i].purge();
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
        }	
        mStopwatch.reset();

		try{
            java.net.URL path = ClassLoader.getSystemResource("unique_movies_list.txt");
            FileReader fr = new FileReader (path.getFile());
            BufferedReader br = new BufferedReader (fr);
            String line;
            
            while ((line = br.readLine()) != null && count < 1000){
                int flip = mRandom.nextInt(2);
                if (1 == flip) {
                    continue;
                }
                
            	int machineClientId = mRandom.nextInt(mServerCount);
                int machineId = machineClientId + 1;
                IInsertDeleteRequest req = new InsertDeleteRequest(mRequestId++, machineId, line, line);
                mStopwatch.start(); 
                mDhtClientArray[machineClientId].insert(req);
                mStopwatch.stop(); 
                System.out.println("DHTServer[" + machineId + "] insert took " + mStopwatch.getElapsedTime());
                count += 1;
    	    }
            
        } catch (FileNotFoundException e2) {
            e2.printStackTrace();
            System.exit(-1);
        } catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		count = 0;
		for (int i = 0; i < mServerCount; i++) {
            try {
                count =  mDhtClientArray[i].count();
                System.out.println("DHTServer[" + (i + 1) + "] count " + count);
                
            } catch (RemoteException e) {
                e.printStackTrace();
                System.out.println("dhtClient: " +  e.getMessage());
            }
            total += count;
        }
//		System.out.println("Total: " + total);
		assertTrue(1000 <= total);
	}
}
