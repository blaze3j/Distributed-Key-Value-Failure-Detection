package distributed.hash.table;

import java.rmi.RemoteException;

/** 
 * Implementation of RMI query request
 */
public class QueryRequest implements IQueryRequest{
	private static final long serialVersionUID = 1L;
	private int RequestId;
	private int MachineId;
	private String Key;
	private String message;
	
	/** 
     * Constructor
     */
	public QueryRequest(int requestId, int machineId, String key){
		this.RequestId = requestId;
		this.MachineId = machineId;
		this.Key = key;
		this.message = "";
	}
	
	/** 
     * Get key of the request
     */
	public String getKey(){ return this.Key;}
	
	/** 
     * Get request id of the request
     */
	public int getRequestId(){ return this.RequestId;}
	
	/** 
     * Get machine id of the request
     */
	public int getMachineId(){ return this.MachineId;}
	
	/** 
     * Append message to this request 
     */
	public void appendMessage(String message){this.message += message;}
	
	/** 
     * Get message of the request
     */
	public String getMessage(){return this.message;}
		
	/** 
     * Generate and return a user friendly string of the request
     */
	public String printRequest() throws RemoteException {return "request " + this.getRequestId() + 
			" from machine " + this.getMachineId() + 
			" for key " + this.getKey();
	}
}