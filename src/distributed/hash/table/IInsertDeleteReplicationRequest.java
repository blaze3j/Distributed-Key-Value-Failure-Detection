package distributed.hash.table;

import java.rmi.RemoteException;

public interface IInsertDeleteReplicationRequest extends IReplicationQueryRequest, IInsertDeleteRequest {
	public boolean isDirty() 
			throws RemoteException;
}
