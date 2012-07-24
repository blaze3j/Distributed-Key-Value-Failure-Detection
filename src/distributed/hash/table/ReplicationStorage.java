package distributed.hash.table;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class ReplicationStorage {

	public int id;
	public String address;
	public int port;
    private Hashtable<String, List<String>> cache;
    
    public ReplicationStorage(int id, String address, int port){
    	this.id = id;
    	this.address = address;
    	this.port = port;
    }
    
    public void insert(String key, String value){
    	utils.Output.print("insert: replication " + this.id + " is inserted");
        List<String> values = this.cache.get(key);
        if(values == null)
        	values = new ArrayList<String>();
        values.add(value);
        this.cache.put(key, values);
    }
}
