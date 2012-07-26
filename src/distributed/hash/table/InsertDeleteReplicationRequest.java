package distributed.hash.table;

import java.rmi.RemoteException;

public class InsertDeleteReplicationRequest extends ReplicationQueryRequest implements IInsertDeleteReplicationRequest{
	private static final long serialVersionUID = 1L;
	Object Value; 

    /** 
     * Constructor
     */
	public InsertDeleteReplicationRequest(int requestId, int machineId, String key, Object value){
		super(requestId, machineId, key);
		this.Value = value;
	}
	
	
	/** 
     * Generate and return a user friendly string of the request
     */
	@Override
	public String printRequest() throws RemoteException{
		return "replication request " + this.getRequestId() + 
			" from machine " + this.getMachineId() + 
			" with <" + this.getKey() + " , " + this.getValue() + ">";
	}

    /** 
     * Get value of the request
     */
	public Object getValue() throws RemoteException { return this.Value; }		
}