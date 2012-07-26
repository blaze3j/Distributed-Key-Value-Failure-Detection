package distributed.hash.table;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/** 
 * This class is a host for replication storage
 */
public class ReplicationStorage {

	public int id;
	public String address;
	public int port;
    private Hashtable<String, List<String>> cache;
    
    /** 
     * Constructor
     */
    public ReplicationStorage(int id, String address, int port){
    	this.id = id;
    	this.address = address;
    	this.port = port;
    	this.cache = new Hashtable<String, List<String>>();
    }
    
    /** 
     * insert a key value to the cache
     */
    public void insert(String key, String value){
        List<String> values = this.cache.get(key);
        if(values == null){
        	values = new ArrayList<String>();
        	this.cache.put(key, values);
        }
        if(!values.contains(value))
        	values.add(value);
        utils.Output.print("insert-ReplicationStorage: machine " + this.id + ", " + key + " is inserted");
    }
    
    /** 
     * get value of a key
     */
    public List<String> getValue(String key){
    	synchronized (this.cache) {
    		return this.cache.get(key);
    	}
    }
    
    /** 
     * remove value from the list of a key
     */
    public boolean remove(String key, String value){
    	synchronized (this.cache) {
	    	List<String> values = this.cache.get(key);
	    	if(values != null && values.contains(value)){
	        	values.remove(value);
	        	if(values.size() == 0)
	        		this.cache.remove(key);
	        	return true;
	    	}
    	}
    	return false;
    }

    /** 
     * clear replication cache
     */
    public void clear(){
    	synchronized (this.cache) {
        	this.cache.clear();	
		}
    }
}
