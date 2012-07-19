package distributed.hash.table;

/** 
 * Implementation of RMI insert request
 */
public class InsertRequest extends QueryRequest implements IInsertRequest{
	private static final long serialVersionUID = 1L;
	Object Value; 

    /** 
     * Constructor
     */
	public InsertRequest(int requestId, int machineId, int key, Object value){
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
