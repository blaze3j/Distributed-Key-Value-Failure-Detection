package failure.detector;

import java.util.EventListener;

public interface ServerJoinListener extends EventListener {
	  public abstract void onServerJoin(ServerJoinEvent e);
}
