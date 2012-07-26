package failure.detector;

import java.util.EventObject;

/**
 * An event that represents the server join to the ring
 */
public class ServerJoinEvent extends EventObject{

	private static final long serialVersionUID = 1L;
	private int id;
	
	public ServerJoinEvent(Object source, int serverId) {
		super(source);
		this.id = serverId;
	}
	
	/** return server id of joint server */
	public int getServerId(){
		return this.id;
	}
}
