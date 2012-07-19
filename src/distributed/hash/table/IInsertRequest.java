package distributed.hash.table;

/** 
 * Interface RMI of insert request
 */
public interface IInsertRequest extends IQueryRequest{
	public Object getValue()
			throws java.rmi.RemoteException;;
}
