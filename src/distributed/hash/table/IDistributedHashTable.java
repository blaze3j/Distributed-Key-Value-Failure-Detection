package distributed.hash.table;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.List;

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
	public boolean purge()
	        throws java.rmi.RemoteException;
	public boolean purgeReplication(IReplicationQueryRequest req)
		throws java.rmi.RemoteException;
	public boolean updateDirtyInsertCache(int senderId, Hashtable<String, List<String>> dirtyInserts)
			throws java.rmi.RemoteException;
	public boolean updateDirtyDeleteCache(int senderId, Hashtable<String, List<String>> dirtyDeletes)
			throws java.rmi.RemoteException;
}
