package failure.detector;

import java.util.EventObject;

public class ServerJoinEvent extends EventObject{

	private static final long serialVersionUID = 1L;
	private int id;
	public ServerJoinEvent(Object source, int serverId) {
		super(source);
		this.id = serverId;
	}
	
	public int getServerId(){
		return this.id;
	}
}
