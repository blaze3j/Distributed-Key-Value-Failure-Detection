package distributed.hash.table;

import java.rmi.RemoteException;
import java.util.*;

public class InsertReplicationRequest extends InsertRequest implements IInsertReplicationRequest{
	private static final long serialVersionUID = 1L;
	private List<Integer> probe; 

    /** 
     * Constructor
     */
	public InsertReplicationRequest(int requestId, int machineId, String key, Object value){
		super(requestId, machineId, key, value);
		this.probe = new ArrayList<Integer>();
	}
	
	public void updateProbe(int id) throws RemoteException{
		this.probe.add(id);
	}	

	public List<Integer> getProbe() throws RemoteException{
		return this.probe;
	}
	
	/** 
     * Generate and return a user friendly string of the request
     */
	@Override
	public String printRequest() {
		return "replication request " + this.getRequestId() + 
			" from machine " + this.getMachineId() + 
			" with <" + this.getKey() + " , " + this.getValue() + ">";
	}		
}