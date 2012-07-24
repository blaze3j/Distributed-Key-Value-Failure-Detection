package distributed.hash.table;

import java.util.List;

public interface IInsertReplicationRequest extends IInsertRequest{
	public List<Integer> getPath()
		throws java.rmi.RemoteException;
	public void updatePath(int machineId)
		throws java.rmi.RemoteException;
}
