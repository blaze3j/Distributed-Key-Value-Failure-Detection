package distributed.hash.table;

import java.rmi.Remote;

/** Interface RMI of distributed hash table
 *  
 */
public interface IDistributedHashTable extends Remote {
	
	public int count() 
			throws java.rmi.RemoteException;
	public void insert(IInsertRequest req)
			throws java.rmi.RemoteException;
	public void delete(IQueryRequest req)
			throws java.rmi.RemoteException;
	public Object lookup(IQueryRequest req)
			throws java.rmi.RemoteException;
    public int lookupTrace(IQueryRequest req)
            throws java.rmi.RemoteException;
	public void purge()
	        throws java.rmi.RemoteException;
}
