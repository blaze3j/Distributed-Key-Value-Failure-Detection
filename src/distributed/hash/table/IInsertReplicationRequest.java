package distributed.hash.table;

import java.util.List;

public interface IInsertReplicationRequest extends IInsertRequest{
	public List<Integer> getProbe()
		throws java.rmi.RemoteException;
	public void updateProbe(int machineId)
		throws java.rmi.RemoteException;
}
