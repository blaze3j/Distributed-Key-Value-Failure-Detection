package distributed.hash.table;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*; 

public class DistributedHashTable extends java.rmi.server.UnicastRemoteObject implements IDistributedHashTable{
    private static final long serialVersionUID = 1L;
    private Hashtable<Integer, Object> cache;
    private LinkedHashMap<String, Integer> successorTable;
    private int myId;
    private int startKey;
    private int keySize;

    /** 
     * Constructor
     */
    public DistributedHashTable(int id, int startKey, int size, LinkedHashMap<String, Integer> successor) throws java.rmi.RemoteException {
        super(); 
        this.cache = new Hashtable<Integer, Object>();
        this.successorTable = successor;
        this.myId = id;
        this.startKey = startKey;
        this.keySize = size;
        
        System.out.print("DHT server id: " + this.myId + " is created.");
		Set<Map.Entry<String, Integer>> peers = successor.entrySet();

		for (Map.Entry<String, Integer> peer : peers) {
			System.out.print(" peer in port: " + peer.getKey()  + " with max: " + peer.getValue() + ".");
		}
        
        System.out.println();
    }
    
    /** 
     * insert an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public void insert(IInsertRequest req) throws RemoteException{
        if(isKeyInThisMachine(req.getKey())){
        	synchronized(this.cache) {
                handelMessage(req, "insert: machine " + this.myId + " - " + req.printRequest() + " is inserted");
                this.cache.put(req.getKey(), req.getValue());
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(req.getKey());
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                handelMessage(req, "insert: machine " + this.myId + " - " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                dhtNextMachine.insert(req);
            }  catch(Exception e) {
                handelMessage(req, "insert: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
            }
        }
    }

    /** 
     * insert an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public Object lookup(IQueryRequest req) throws RemoteException{
    	if(isKeyInThisMachine(req.getKey())){
            synchronized(this.cache) {
                if(this.cache.containsKey(req.getKey())){
                    Object value = this.cache.get(req.getKey());
                    handelMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " is " + value);
                    return value;
                }
                else{
                    handelMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " not found.");				
                    return null;
                }			
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(req.getKey());
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                handelMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                return dhtNextMachine.lookup(req);
            }catch(Exception e) {
                handelMessage(req, "lookup: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
            }
        }
        return null;
    }

    /** 
     * lookup and trace an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public int lookupTrace(IQueryRequest req) throws RemoteException{
    	if(isKeyInThisMachine(req.getKey())){
            synchronized(this.cache) {
                if(this.cache.containsKey(req.getKey())){
                    handelMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " is found");
                    return 1;
                }
                else{
                    handelMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " not found.");               
                    return 0;
                }           
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(req.getKey());
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                handelMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                return 1 + dhtNextMachine.lookupTrace(req);
            }catch(Exception e) {
                handelMessage(req, "lookup trace: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
            }
        }
        return 0;
    }

    /** 
     * delete an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public void delete(IQueryRequest req) throws RemoteException{
    	if(isKeyInThisMachine(req.getKey())){
            synchronized(this.cache) {
                if(this.cache.containsKey(req.getKey())){
                    handelMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " is deleted");					
                    this.cache.remove(req.getKey());
                }
                else{
                    handelMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " not found");
                }
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(req.getKey());
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress+"/DistributedHashTable");
                handelMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                dhtNextMachine.delete(req);
            } catch(Exception e) {
                handelMessage(req, "delete: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
            }
        }
    }

    /** 
     * purge local hash table
     */
    public void purge(){
        synchronized(this.cache) {
            System.out.println("purge: machine " + this.myId + "\n");
            this.cache.clear();
        }
    }

    /** 
     * return number of keys store in the local hash table
     */
    public int count(){
        synchronized(this.cache) {
            int n = this.cache.size();
            System.out.println("count: machine " + this.myId + " is " + n +"\n");
            return n;
        }
    }

    /** 
     * append message to the request
     */
    private void handelMessage(IQueryRequest req, String msg){
        try{
            System.out.println(msg);
            req.appendMessage(msg);
        }catch(Exception e){ }
    }
    
    /** 
     * check if the key is stored in this local hash table
     */
    private boolean isKeyInThisMachine(int key){
    	return this.startKey <= key && key < (this.startKey + this.keySize);
    }
    
    /** 
     * find the next machine from successor table for a key
     * if next machine is not found, return the last server in the successor table
     */  
	private String getNextMachineAddress(int key){
		Set<Map.Entry<String, Integer>> successors = this.successorTable.entrySet();
		Map.Entry<String, Integer> selectedEntry = successors.iterator().next();
		int prev = Integer.MIN_VALUE;
		boolean isSmaller = key < this.startKey;
		if(!isSmaller){
			for (Map.Entry<String, Integer> successor : successors){
				if( prev < key  && key <= successor.getValue())
					return successor.getKey();
				prev = successor.getValue();
				selectedEntry = successor;
			}
		}
		else{
			for (Map.Entry<String, Integer> successor : successors){
				if(successor.getValue() >= key ){
					if(this.startKey > successor.getValue())
						return successor.getKey();
				}
				selectedEntry = successor;
			}
		}
		
		return selectedEntry.getKey();
	}
}