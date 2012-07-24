package distributed.hash.table;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*; 

import failure.detector.FailureDetectorThread;

public class DistributedHashTable extends java.rmi.server.UnicastRemoteObject implements IDistributedHashTable{
	
	private static final List<String> STOP_WORDS = new ArrayList<String>(Arrays.asList("a","able","about","across","after","all","almost","also","am","among","an","and",
			"any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else",
			"ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in",
			"into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor",
			"not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so",
			"some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was",
			"we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"));

    private static final long serialVersionUID = 1L;
    private Hashtable<String, List<String>> cache;
    private List<ReplicationStorage> replications;
    private LinkedHashMap<Integer, String> successorTable; // <id, address>
    private int myId;
    private int sCount;

    /** 
     * Constructor
     */
    public DistributedHashTable(int id, int serverCount, LinkedHashMap<Integer, String> successor) throws java.rmi.RemoteException {
        super(); 
        this.cache = new Hashtable<String, List<String>>();
        replications = new ArrayList<ReplicationStorage>();
        this.successorTable = successor;

        this.myId = id;
        this.sCount = serverCount;
        
        utils.Output.println("DHT server id: " + this.myId + " is created.");
        Set<Map.Entry<Integer, String>> peers = successor.entrySet();

		for (Map.Entry<Integer, String> peer : peers) {
			// <id, address> address can be name:port. For now assumption is address is just port
			replications.add(new ReplicationStorage(peer.getKey(), "", Integer.parseInt(peer.getValue())));
			utils.Output.println(" peer " + peer.getKey()  + " on port " + peer.getValue() + " is added.");
		}
    }
    
    /** 
     * insert an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public void insert(IInsertRequest req) throws RemoteException{
    	for(String word: splitWithStopWords(req.getKey().toLowerCase())){
            if(getServer(word) == this.myId){
            	// update local copy
            	synchronized(this.cache) {
                    handleMessage(req, "insert: machine " + this.myId + " - request " + req.getRequestId() + " from machine " + req.getMachineId() + " with <" + word + " , " + req.getValue() + ">  is inserted.\n");
                    String newValue = (String)req.getValue();
                    List<String> values = this.cache.get(word);
                    if(values == null)
                    	values = new ArrayList<String>();
                    if(values.contains(newValue))
                    	continue;
                    values.add(newValue);
                    this.cache.put(word, values);
                }
            	// update its replations
            }
            else{
                try {
                	IInsertRequest reqNextMachine = new InsertRequest(req.getRequestId(), req.getMachineId() , word, req.getValue());
                	String nextMachineAddress = getNextMachineAddress(word);
                    IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                    Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                    handleMessage(req, "insert: machine " + this.myId + " - " + reqNextMachine.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                    UnicastRemoteObject.exportObject(reqNextMachine);
                    dhtNextMachine.insert(reqNextMachine);
                    handleMessage(req, reqNextMachine.getMessage());
                }  catch(Exception e) {
                    handleMessage(req, "insert: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
                }
            }
    	}
    }
    
	@Override
	public void insertReplication(IInsertReplicationRequest req) throws RemoteException {
    	synchronized (replications) {
        	for(ReplicationStorage rep: replications){
        		String key = req.getKey();
        		if(getServer(key) == rep.id){
        			rep.insert(key, (String)req.getValue());
        		}
        	}	
		}
    }

    /** 
     * insert an entity on the local hash table if it in the range of this machine,
     * or send the request to the next server that key belongs to if it is not in this server.
     * if next server can not be located, send it to the last server
     */
    public Object lookup(IQueryRequest req) throws RemoteException{
    	String key = req.getKey().toLowerCase();
    	if(getServer(key) == this.myId){
            synchronized(this.cache) {
                if(this.cache.containsKey(key)){
                    Object value = this.cache.get(key);
                    handleMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " is " + value);
                    return value;
                }
                else{
                    handleMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " not found.");				
                    return null;
                }			
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(key);
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                handleMessage(req, "lookup: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                return dhtNextMachine.lookup(req);
            }catch(Exception e) {
                handleMessage(req, "lookup: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
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
    	String key = req.getKey().toLowerCase();
    	if(getServer(key) == this.myId){
            synchronized(this.cache) {
                if(this.cache.containsKey(key)){
                    handleMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " is found");
                    return 1;
                }
                else{
                    handleMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " not found.");               
                    return 0;
                }           
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(key);
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress +"/DistributedHashTable");
                handleMessage(req, "lookup trace: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                return 1 + dhtNextMachine.lookupTrace(req);
            }catch(Exception e) {
                handleMessage(req, "lookup trace: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
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
    	String key = req.getKey().toLowerCase();
    	if(getServer(key) == this.myId){
            synchronized(this.cache) {
                if(this.cache.containsKey(key)){
                    handleMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " is deleted");					
                    this.cache.remove(key);
                }
                else{
                    handleMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " not found");
                }
            }
        }
        else{
            try {
            	String nextMachineAddress = getNextMachineAddress(key);
                IDistributedHashTable dhtNextMachine = (IDistributedHashTable) 
                Naming.lookup("rmi://localhost:"+ nextMachineAddress+"/DistributedHashTable");
                handleMessage(req, "delete: machine " + this.myId + " - value of " + req.printRequest() + " routed to machine address " + nextMachineAddress + "\n");
                dhtNextMachine.delete(req);
            } catch(Exception e) {
                handleMessage(req, "delete: machine " + this.myId + " - dhtNextMachine: " +  e.getMessage());
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
    private void handleMessage(IQueryRequest req, String msg){
        try{
            System.out.println(msg);
            req.appendMessage(msg);
        }catch(Exception e){ }
    }
    
    /** 
     * calculate the hash code of the key and return the server associated to the key
     */   
    private int getServer(String key){
    	int hash = key.hashCode();
    	int server =(hash % this.sCount) + 1;
    	utils.Output.println("****** Hash cod of " + key + " is " + hash + " server " + server);
    	return (server < 0) ? server + this.sCount : server;
    }
    
    /** 
     * find the next machine from successor table for a key
     * if next machine is not found, return the last server in the successor table
     */
	private String getNextMachineAddress(String key){
		Map.Entry<Integer, String> nextMachine = null;
		int server = getServer(key);
		for (Map.Entry<Integer, String> peer : this.successorTable.entrySet()) {
			int id = peer.getKey();
			// check if it is alive
			//if(FailureDetector.isAlive(id)){
				if(server == id){					
					return peer.getValue();
				}
				nextMachine = peer;
			//}
		} 
		// send the request to the last machine in successor 
		if(nextMachine != null)
			return nextMachine.getValue();
		// if all successors are off line, then send the request to the back up peer.
		return null;//FailureDetector.getBackupPeer().getValue();
		
	}
	
	private String[] splitWithStopWords(String s){
		List<String> res = new ArrayList<String>();
		for(String word: s.split(" "))
		{
			if(STOP_WORDS.contains(word) == false){
				res.add(word);
			}
		}
		
		return res.toArray(new String[res.size()]);
	}
}