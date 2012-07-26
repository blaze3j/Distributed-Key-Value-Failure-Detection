package failure.detector;

import java.util.EventListener;


/** A contract between a ServerJoinEvent source and
  *   listener classes
  */
public interface ServerJoinListener extends EventListener {
	  public abstract void onServerJoin(ServerJoinEvent e);
}
