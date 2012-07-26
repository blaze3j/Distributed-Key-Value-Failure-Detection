package distributed.hash.table;

import java.rmi.RemoteException;
import java.util.List;

public interface IReplicationQueryRequest extends IQueryRequest{
	public List<Integer> getProbe()
		throws java.rmi.RemoteException;
	public void updateProbe(int machineId)
		throws java.rmi.RemoteException;
	public int getUpdateCount()
		throws java.rmi.RemoteException;
	public void incrementUpdateCount() 
		throws RemoteException;
}
