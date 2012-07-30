package distributed.hash.table;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/** 
 * This class is a host for replication storage
 */
public class ReplicationStorage {

	private int id;
	private String address;
	private int port;
    private Hashtable<String, ArrayList<String>> localCache;
    private Hashtable<String, ArrayList<String>> dirtyInsertCache;
    private Hashtable<String, ArrayList<String>> dirtyDeleteCache;
    
    /** 
     * Constructor
     */
    public ReplicationStorage(int id, String address, int port){
    	this.id = id;
    	this.address = address;
    	this.port = port;
    	this.localCache = new Hashtable<String, ArrayList<String>>();
    	this.dirtyInsertCache = new Hashtable<String, ArrayList<String>>();
    	this.dirtyDeleteCache = new Hashtable<String, ArrayList<String>>();
    }
    
    /** 
     * insert a key value to the cache
     */
    public void insert(String key, String value, boolean isDirty){
    	if(isDirty){
    		insert(this.dirtyInsertCache, key, value);
    		handleMessage("insert dirty - ReplicationStorage: machine " + this.id + ", " + key + " is inserted");
    	}
    	if(insert(this.localCache, key, value))
    		handleMessage("insert - ReplicationStorage: machine " + this.id + ", " + key + " is inserted");
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
    		handleMessage("remove dirty - ReplicationStorage: machine " + this.id + ", " + key + " is deleted");
    	}
    	
    	if(remove(this.localCache, key, value)){
    		handleMessage("remove - ReplicationStorage: machine " + this.id + ", " + key + " is deleted");
    		return true;
    	}
    	return false;
    }

    /** 
     * get id 
     */
    public int getId()
    {
        return this.id;
    }
    
    /** 
     * get address 
     */
    public String getAddress()
    {
        return this.address;
    }
    
    /** 
     * get id 
     */
    public int getPort()
    {
        return this.port;
    }
    
    /** 
     * get list of local
     */
    public Hashtable<String, ArrayList<String>> getLocalCache(){
        return this.localCache;
    }
    /** 
     * get list of dirty inserts 
     */
    public Hashtable<String, ArrayList<String>> getDirtyInsertCache(){
        return this.dirtyInsertCache;
    }
    
    /** 
     * get list of dirty deletes 
     */
    public Hashtable<String, ArrayList<String>> getDirtyDeleteCache(){
        return this.dirtyDeleteCache;
    }
    
    /** 
     * set list of locals 
     */
    public void setLocalCache(Hashtable<String, ArrayList<String>> hashtable){
        this.localCache = hashtable;
    }
    
    /** 
     * clear dirty inserts
     */
    public void clearDirtyInsert(){
		handleMessage("clearDirtyInsert - ReplicationStorage: machine " + this.id);
    	synchronized (this.dirtyInsertCache) {
        	this.dirtyInsertCache.clear();
		}
    }
     
     /** 
      * clear dirty deletes
      */
     public void clearDirtyDelete(){
 		handleMessage("clearDirtyDelete - ReplicationStorage: machine " + this.id);
     	synchronized (this.dirtyDeleteCache) {
         	this.dirtyDeleteCache.clear();
 		}
     }
    
    /** 
     * clear replication cache
     */
    public void clear(){
 		handleMessage("clear - ReplicationStorage: machine " + this.id);
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
    private boolean insert(Hashtable<String, ArrayList<String>> cache, String key, String value){
    	synchronized (cache) {
    		ArrayList<String> values = cache.get(key);
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
    private boolean remove(Hashtable<String, ArrayList<String>> cache, String key, String value){
    	synchronized (cache) {
    		ArrayList<String> values = cache.get(key);
	    	if(values != null && values.contains(value)){
	        	values.remove(value);
	        	if(values.size() == 0)
	        		cache.remove(key);
	        	return true;
	    	}
    	}
    	return false;
    }
    
    /** 
     * append message to the request
     */
    private void handleMessage( String msg){
        try{
            if(DistributedHashTable.DebugMode){
            	utils.Output.println(msg);
            }
        }catch(Exception e){ }
    }
}
