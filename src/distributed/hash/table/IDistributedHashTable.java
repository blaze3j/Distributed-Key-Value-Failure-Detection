package distributed.hash.table;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

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
    public int lookupTrace(IQueryRequest req)
        throws java.rmi.RemoteException;
	public boolean purge()
	    throws java.rmi.RemoteException;
	public boolean purgeReplication(IReplicationQueryRequest req)
		throws java.rmi.RemoteException;
	public boolean syncDirtyInsertCache(int senderId, Hashtable<String, ArrayList<String>> dirtyInserts, boolean isDirty)
		throws java.rmi.RemoteException;
	public boolean syncDirtyDeleteCache(int senderId, Hashtable<String, ArrayList<String>> dirtyDeletes, boolean isDirty)
		throws java.rmi.RemoteException;	
	public Hashtable<String, ArrayList<String>> getDirtyDeleteCache(int newJoindServerId)
		throws java.rmi.RemoteException;
	public Hashtable<String, ArrayList<String>> getDirtyInsertCache(int newJoindServerId)
		throws java.rmi.RemoteException;
	public Hashtable<String, ArrayList<String>> getDirtyInsertRepCache(int serverId)
		throws RemoteException;
	public Hashtable<String, ArrayList<String>> getDirtyDeleteRepCache(int serverId)
		throws RemoteException;	
	public List<Integer> getReplicationServers(int serverId) 
		throws RemoteException;
	public Map.Entry<Integer, String> getRepHostAddress(int requestorId, int serverId)
		throws RemoteException;
}
