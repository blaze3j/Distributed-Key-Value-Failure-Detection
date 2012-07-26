package distributed.hash.table;

/** 
 * Implementation of RMI insert request
 */
public class InsertDeleteRequest extends QueryRequest implements IInsertDeleteRequest{
	private static final long serialVersionUID = 1L;
	Object Value; 

    /** 
     * Constructor
     */
	public InsertDeleteRequest(int requestId, int machineId, String key, Object value){
		super(requestId, machineId, key);
		this.Value = value;
	}

    /** 
     * Get value of the request
     */
	public Object getValue(){return this.Value;}
	
    /** 
     * Generate and return a user friendly string of the request
     */
	public String printRequest() {return "request " + this.getRequestId() + 
			" from machine " + this.getMachineId() + 
			" with <" + this.getKey() + " , " + this.getValue() + ">";
	}
}
