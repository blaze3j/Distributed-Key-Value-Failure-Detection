package distributed.hash.table;

import java.rmi.RemoteException;
import java.util.*;

public class InsertReplicationRequest extends InsertRequest implements IInsertReplicationRequest{
	private static final long serialVersionUID = 1L;
	private List<Integer> Path; 

    /** 
     * Constructor
     */
	public InsertReplicationRequest(int requestId, int machineId, String key, Object value){
		super(requestId, machineId, key, value);
		Path = new ArrayList<Integer>();
	}
	
	@Override
	public void updatePath(int id){
		Path.add(id);
	}	

	@Override
	public List<Integer> getPath() throws RemoteException {
		return this.Path;
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
