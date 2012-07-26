package distributed.hash.table;

import java.rmi.Remote;
import java.rmi.RemoteException;

/** Interface RMI of distributed hash table
 *  
 */
public interface IDistributedHashTable extends Remote {
	
	public int count() 
			throws java.rmi.RemoteException;
	public void insert(IInsertDeleteRequest req)
			throws java.rmi.RemoteException;
	public void insertReplication(IInsertDeleteReplicationRequest req)
		throws java.rmi.RemoteException;
	public void delete(IInsertDeleteRequest req)
			throws java.rmi.RemoteException;
	public void deleteReplication(IInsertDeleteReplicationRequest req) 
		throws RemoteException;
	public Object lookup(IQueryRequest req)
			throws java.rmi.RemoteException;
	public Object lookupReplication(IReplicationQueryRequest req) 
		throws RemoteException;
    public int lookupTrace(IQueryRequest req)
            throws java.rmi.RemoteException;
	public void purge(IQueryRequest req)
	        throws java.rmi.RemoteException;
	public void purgeReplication(IReplicationQueryRequest req)
		throws java.rmi.RemoteException;
	
}
