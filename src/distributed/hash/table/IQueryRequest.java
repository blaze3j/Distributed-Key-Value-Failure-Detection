package distributed.hash.table;

import java.rmi.Remote;

/** 
 * Interface RMI of query request
 */
public interface IQueryRequest extends java.io.Serializable, Remote{
	public int getKey() 
			throws java.rmi.RemoteException;
	public int getRequestId() 
			throws java.rmi.RemoteException;
	public int getMachineId() 
			throws java.rmi.RemoteException; 
	public void appendMessage(String message) 
			throws java.rmi.RemoteException;
	public String getMessage()
			throws java.rmi.RemoteException;
	public String printRequest()
			throws java.rmi.RemoteException;
}