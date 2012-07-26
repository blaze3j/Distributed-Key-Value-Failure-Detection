import java.io.IOException;
import com.sun.org.apache.xalan.internal.xsltc.cmdline.getopt.GetOpt;

import failure.detector.*;

public class FailureDetectorServer {
	private static int serverId;
	private static FailureDetectorThread failureDetectorThread = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String failureSettingFile = "";
		GetOpt getopt = new GetOpt(args, "i:f:");
		serverId = 1;
		try {
			int c;
			while ((c = getopt.getNextOption()) != -1) {
			    switch(c) {
			    case 'i':
			    	serverId = Integer.parseInt(getopt.getOptionArg());
			        break;
			    case 'f':
			    	failureSettingFile = getopt.getOptionArg();
			        break;
			    }
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		try {
            failureDetectorThread = FailureDetectorThread.getInstance(serverId, failureSettingFile);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
		failureDetectorThread.run();
		
		while(failureDetectorThread.isAlive());
	}
}
