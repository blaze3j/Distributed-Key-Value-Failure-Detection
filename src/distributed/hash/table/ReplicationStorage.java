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
    private Hashtable<String, List<String>> localCache;
    private Hashtable<String, List<String>> dirtyInsertCache;
    private Hashtable<String, List<String>> dirtyDeleteCache;
    
    /** 
     * Constructor
     */
    public ReplicationStorage(int id, String address, int port){
    	this.id = id;
    	this.address = address;
    	this.port = port;
    	this.localCache = new Hashtable<String, List<String>>();
    	this.dirtyInsertCache = new Hashtable<String, List<String>>();
    	this.dirtyDeleteCache = new Hashtable<String, List<String>>();
    }
    
    /** 
     * insert a key value to the cache
     */
    public void insert(String key, String value, boolean isDirty){
    	if(isDirty){
    		insert(this.dirtyInsertCache, key, value);
    		utils.Output.print("insert dirty - ReplicationStorage: machine " + this.id + ", " + key + " is inserted\n");
    	}
    	if(insert(this.localCache, key, value))
    		utils.Output.print("insert - ReplicationStorage: machine " + this.id + ", " + key + " is inserted\n");
    }
    
    /** 
     * get value of a key
     */
    public List<String> getValue(String key){
    	synchronized (this.localCache) {
    		return this.localCache.get(key);
    	}
    }
    
    /** 
     * remove value from the list of a key
     */
    public boolean remove(String key, String value, boolean isDirty){
    	if(isDirty){
    		insert(this.dirtyDeleteCache, key, value);
    		utils.Output.print("remove dirty - ReplicationStorage: machine " + this.id + ", " + key + " is deleted\n");
    	}
    	
    	if(remove(this.localCache, key, value)){
    		utils.Output.print("remove - ReplicationStorage: machine " + this.id + ", " + key + " is deleted\n");
    		return true;
    	}
    	return false;
    }

    /** 
     * get list of dirty inserts 
     */
    public Hashtable<String, List<String>> getDirtyInsertCache(){
    	return this.dirtyInsertCache;
    }
    
    /** 
     * get list of dirty deletes 
     */
    public Hashtable<String, List<String>> getDirtyDeleteCache(){
    	return this.dirtyDeleteCache;
    }
    
    /** 
     * clear dirty inserts
     */
    public void clearDirtyInsert(){
    	synchronized (this.dirtyInsertCache) {
        	this.dirtyInsertCache.clear();
		}
    }
     
     /** 
      * clear dirty deletes
      */
     public void clearDirtyDelete(){
     	synchronized (this.dirtyDeleteCache) {
         	this.dirtyDeleteCache.clear();
 		}
     }
    
    /** 
     * clear replication cache
     */
    public void clear(){
    	synchronized (this.localCache) {
        	this.localCache.clear();
		}
    	synchronized (this.dirtyInsertCache) {
        	this.dirtyInsertCache.clear();
		}
    	synchronized (this.dirtyDeleteCache) {
        	this.dirtyDeleteCache.clear();
		}
    }

    /** 
     * insert an entity to a cache
     */
    private boolean insert(Hashtable<String, List<String>> cache, String key, String value){
    	synchronized (cache) {
        	List<String> values = cache.get(key);
            if(values == null){
            	values = new ArrayList<String>();
            	cache.put(key, values);
            }
            if(!values.contains(value)){
            	values.add(value);
            	return true;
            }
		}
    	return false;
    }
    
    /** 
     * remove an entity to a cache
     */
    private boolean remove(Hashtable<String, List<String>> cache, String key, String value){
    	synchronized (cache) {
	    	List<String> values = cache.get(key);
	    	if(values != null && values.contains(value)){
	        	values.remove(value);
	        	if(values.size() == 0)
	        		cache.remove(key);
	        	return true;
	    	}
    	}
    	return false;
    }
}
