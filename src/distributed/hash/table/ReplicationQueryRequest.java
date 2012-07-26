package distributed.hash.table;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationQueryRequest extends QueryRequest implements IReplicationQueryRequest {
		private static final long serialVersionUID = 1L;
	// stores the list of visited servers for this insertion
	private List<Integer> probe; 
	// stores number of updates for this insertion
	private AtomicInteger updateCount;

	public ReplicationQueryRequest(int requestId, int machineId, String key) {
		super(requestId, machineId, key);
		this.probe = new ArrayList<Integer>();
		this.updateCount = new AtomicInteger(0);
	}

	/** 
     * Adds a new server to the probe
     */
	public void updateProbe(int id) throws RemoteException{
		synchronized (this.probe) {
			this.probe.add(id);	
		}
	}	

	/** 
     * Returns list of visited servers
     */
	public List<Integer> getProbe() throws RemoteException{
		return this.probe;
	}
	
	/** 
     * Returns the number of insertion for this request
     */
	public int getUpdateCount() throws RemoteException{
		return this.updateCount.get();
	}
	
	/** 
     * Increments the insertion count
     */
	public void incrementUpdateCount() throws RemoteException{
		this.updateCount.incrementAndGet();
	}
}
