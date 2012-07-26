package distributed.hash.table;

/** 
 * Interface RMI of insert request
 */
public interface IInsertDeleteRequest extends IQueryRequest{
	public Object getValue()
			throws java.rmi.RemoteException;
}
