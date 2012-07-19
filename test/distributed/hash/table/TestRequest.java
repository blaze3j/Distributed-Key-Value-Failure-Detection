package distributed.hash.table;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * JUnit test to distributed hash table functions 
 */
public class TestRequest extends TestExperiment {
	
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
    public void testExperiment1() {
        try {
            IInsertRequest iReq = new InsertRequest(mRequestId++, 1, 1, 1);
            mDhtClientArray[0].insert(iReq);
            System.out.println("DHTClient insert: " + iReq.getMessage());

            iReq = new InsertRequest(mRequestId++, 3, 2, 2);
            mDhtClientArray[2].insert(iReq);
            System.out.println("DHTClient insert: " + iReq.getMessage());

            iReq = new InsertRequest(mRequestId++, 4, 982345, 756321);
            mDhtClientArray[3].insert(iReq);
            System.out.println("DHTClient insert: " + iReq.getMessage());

            IQueryRequest qReq = new QueryRequest(mRequestId++, 4, 2);
            Object value = mDhtClientArray[3].lookup(qReq);
            System.out.println("DHTClient lookup: " + value.toString());

            System.out.println("DHTClient get Count on machine id 1 is " + mDhtClientArray[0].count());
            
            Integer intValue = new Integer(value.toString());
            
            assertTrue(intValue.intValue() == 2);
        }  catch(Exception e) {
            System.out.println("dhtClient: " +  e.getMessage());
        }
    }
}