import java.awt.*;
import java.awt.event.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import javax.swing.*;
import com.sun.org.apache.xalan.internal.xsltc.cmdline.getopt.GetOpt;

import distributed.hash.table.*;

/**
 * Interactive Client application for Distributed hash table
 */
public class DHTInteractiveClient extends JFrame{
	private static boolean DebugMode = false;
	
    private int mServerCount;
    public int[] mPortMap;
    private int mRequestId = 1;
    private String clientSettingFile;

	private static final long serialVersionUID = 1L;
	private JTextField queryBox;
	private JTextField serverBox;
	private JLabel queryLabel;
	private JLabel serverLabel;
	private JLabel serverOutput;
	private JButton execButton;
	private JButton purgeButton;
	private JButton clearButton;
	private Panel resultPanel;
	private JTextArea resultArea;
	private JScrollPane scrollingArea;
	private Panel upperPanel;
	private JRadioButton deleteRadionButton;
	private JRadioButton insertRadionButton;
	private JRadioButton lookupRadionButton;
	private JRadioButton countRadionButton;
    private ButtonGroup actionGroup;

    // Constructor
	public DHTInteractiveClient(String settingFile) {
		this.clientSettingFile = settingFile;
	}

	// initialize client UI
	private void initComponents(){
		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setTitle("Distributed Hash Table Client");
		queryBox = new JTextField(25);
		serverBox = new JTextField(2);
		queryLabel = new JLabel("Query: ");
		queryLabel.setLabelFor(queryBox);
		serverLabel = new JLabel("Server Id: ");
		serverLabel.setLabelFor(serverBox);
		serverOutput = new JLabel("Server Output:"); 

		JPanel pane = new JPanel(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 0;
		pane.add(queryLabel, c);		
		c.gridx = 1;
		pane.add(queryBox, c);
				
		c.gridx = 0;
		c.gridy = 1;
		pane.add(serverLabel, c);		
		c.gridx = 1;
		pane.add(serverBox, c);
		
		RadioListener radioListener = new RadioListener();
		actionGroup = new ButtonGroup();
		insertRadionButton = new JRadioButton("Insert");
		insertRadionButton.setSelected(true);
		insertRadionButton.setActionCommand("insert");
		insertRadionButton.addActionListener(radioListener);
		lookupRadionButton = new JRadioButton("Lookup");
		lookupRadionButton.setActionCommand("lookup");
		lookupRadionButton.addActionListener(radioListener);
		deleteRadionButton = new JRadioButton("Delete");
		deleteRadionButton.setActionCommand("delete");
		deleteRadionButton.addActionListener(radioListener);
		countRadionButton = new JRadioButton("Count");
		countRadionButton.setActionCommand("count");
		countRadionButton.addActionListener(radioListener);
		actionGroup.add(insertRadionButton);
		actionGroup.add(lookupRadionButton);
		actionGroup.add(deleteRadionButton);
		actionGroup.add(countRadionButton);
		
		c.gridx = 2;
		c.gridy = 0;
		pane.add(insertRadionButton, c);		
		c.gridy = 1;
		pane.add(lookupRadionButton, c);
		c.gridy = 2;
		pane.add(deleteRadionButton, c);
		c.gridy = 3;
		pane.add(countRadionButton, c);


		execButton= new JButton("Execute");
		execButton.addActionListener(new ExecButtonListener());
		c.gridx = 3;
		c.gridy = 0;
		pane.add(execButton, c);
		
		purgeButton	= new JButton("Purge");
		purgeButton.addActionListener(new PurgeButtonListener());
		c.gridx = 3;
		c.gridy = 2;
		pane.add(purgeButton, c);
		
		clearButton	= new JButton("Clear Text");
		clearButton.addActionListener(new ClearButtonListener());
		
		resultArea = new JTextArea(28, 70);
		resultArea.setEditable(false);
		scrollingArea = new JScrollPane(resultArea);
		
		upperPanel = new Panel(new BorderLayout());
		upperPanel.add(pane, BorderLayout.CENTER);
		
		resultPanel = new Panel();
		resultPanel.setLayout (new FlowLayout(FlowLayout.CENTER));
		resultPanel.add(serverOutput);
		resultPanel.add(clearButton);
		resultPanel.add(scrollingArea);
		
		add(upperPanel, BorderLayout.NORTH);
	    add(resultPanel, BorderLayout.CENTER);
		setSize(800, 600);
        this.setVisible(true);
	}
	
	// initialize data hash table servers
	// read server addresses from file and initialize the servers
	private void initServers(){
		
		try{
			java.net.URL path = ClassLoader.getSystemResource(clientSettingFile);	
			FileReader fr = new FileReader (path.getFile());
	        BufferedReader br = new BufferedReader (fr);
	        try {
				String[] portMap = br.readLine().split(",");
				mServerCount = portMap.length;
				mPortMap = new int[mServerCount];
				for(int i = 0; i < mServerCount; i++){
					mPortMap[i] = Integer.parseInt(portMap[i]);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
			System.exit(-1);
		}
		
        for (int i = 0; i < mServerCount; i++) {
        	try{
            Naming.lookup("rmi://localhost:" + mPortMap[i] + "/DistributedHashTable");
    		appendOutput("server: " + (i+1) + " is connected");
        	}catch(Exception e) {
        		appendOutput("initServers: " + (i+1) + " " +  e.getMessage());
        	}
        }
	}
	
	private IDistributedHashTable getServer(int id) throws Exception{
		return (IDistributedHashTable) Naming.lookup("rmi://localhost:" + mPortMap[id-1] + "/DistributedHashTable");
	}
	
	// send an insert request to a server
	// server: id of server to insert the query
	private void sendInsertRequest(String query, Object value, int server)
	{
		IInsertDeleteRequest insReq = new InsertDeleteRequest(mRequestId++, server, query, value);
		try {
			IDistributedHashTable dhtServer = getServer(server);
			if(DebugMode)
				UnicastRemoteObject.exportObject(insReq);
			dhtServer.insert(insReq);
			if(DebugMode)
				appendOutput("DHT Server:\n" + insReq.getMessage());
		} catch (Exception e) {
			appendOutput("sendInsertRequest: " + server +  e.getMessage());
		}
	}
	
	// send a delete request to a server
	// server: id of server to delete the query
	private void sendDeleteRequest(String query, int server)
	{
		IInsertDeleteRequest queryReq = new InsertDeleteRequest(mRequestId++, server, query, query);
		try {
			IDistributedHashTable dhtServer = getServer(server);
			if(DebugMode)
				UnicastRemoteObject.exportObject(queryReq);
			dhtServer .delete(queryReq);
			if(DebugMode)
				appendOutput("DHT Server:\n" + queryReq.getMessage());
		} catch (Exception e) {
			appendOutput("sendDeleteRequest: " + server +  e.getMessage());
		}
	}
	
	// send a lookup request to a server
	// server: id of server to lookup the query
	private void sendLookupRequest(String query, int server)
	{
		IQueryRequest queryReq = new QueryRequest(mRequestId++, server, query);
		try {
			IDistributedHashTable dhtServer = getServer(server);		
			if(DebugMode)
				UnicastRemoteObject.exportObject(queryReq);
			ArrayList<String> values = (ArrayList<String>)dhtServer.lookup(queryReq);
			String msg = "";
			if(values != null){
				for(String value: values)
					msg += "\t" + value + " \n";
				msg = msg.substring(0, msg.length() - 2);
			}
			else
				msg += "\tnull";
			if(DebugMode)
				appendOutput("DHT Server:\n" + queryReq.getMessage() + "\n");
			appendOutput("DHT Client:\nlookup value is:\n" + msg );
			
		} catch (Exception e) {
			appendOutput("sendLookupRequest: " + server +  e.getMessage());
		}
	}
	
	// send a count request to a server
	// server: id of server to count the number of entities
	private void sendCountRequest(int server)
	{
		try {
			IDistributedHashTable dhtServer = getServer(server);
			int n = dhtServer.count();
			appendOutput("Count machine " + server + " is " + n);
		} catch (Exception e) {
			appendOutput("sendCountRequest: " + server +  e.getMessage());
		}
	}

	// send a purge request to a server
	// server: id of server to purge data
	private void sendPurgeRequest()
	{
		for(int i = 1; i <= mServerCount; i++){
			try {
				IDistributedHashTable dhtServer = getServer(i);
				if(dhtServer.purge())
					appendOutput("DHT Client purge machine " + (i) + " is done.\n");
				else
					appendOutput("DHT Client purge machine " + (i) + " is failed.\n");
			} catch (Exception e1) {
				appendOutput("purge server " + (i) + " " + e1.getMessage());
			}	
		}
	}
	
	// append string to the output text area
	private void appendOutput(String msg){
		resultArea.append(msg + "\n");
		resultArea.append("             ******************************\n");
	}
	
	// radio button listener
	class RadioListener implements ActionListener { 
        public void actionPerformed(ActionEvent e) {
        	String action = e.getActionCommand();
        	if(action == "insert"){
        		queryBox.setEnabled(true);
        	}
        	else if(action == "lookup"){
        		queryBox.setEnabled(true);
        	}
        	else if(action == "delete"){
        		queryBox.setEnabled(true);
        	}
        	else if(action == "count"){
        		queryBox.setText("");
        		queryBox.setEnabled(false);
        	}
        }
	}
	
	
	// clear button listener
	class ClearButtonListener implements ActionListener { 
        public void actionPerformed(ActionEvent e) {
        	resultArea.setText("");
        }
	}
	
	// execute button listener
	class ExecButtonListener implements ActionListener { 
        public void actionPerformed(ActionEvent e) {
        	String action = actionGroup.getSelection().getActionCommand();
        	String query = queryBox.getText();
        	String server = serverBox.getText();
        	if(action == "insert"){
        		if(!validateQuery()){
        			return;
        		}
        		if(!validateServer()){
        			return;
        		}
        		sendInsertRequest(query, query, Integer.parseInt(server));
        	}
        	else if(action == "lookup"){
        		if(!validateQuery()){
        			return;
        		}
        		if(!validateServer()){
        			return;
        		}
        		sendLookupRequest(query, Integer.parseInt(server));
        	}
        	else if(action == "delete"){
        		if(!validateQuery()){
        			return;
        		}
        		if(!validateServer()){
        			return;
        		}
        		sendDeleteRequest(query, Integer.parseInt(server));
        	}
        	else if(action == "count"){
        		if(!validateServer()){
        			return;
        		}
        		sendCountRequest(Integer.parseInt(server));
        	}
        }
        
        private boolean validateServer(){
        	String server = serverBox.getText();
        	if(server.isEmpty()){
    			JOptionPane.showMessageDialog(null, "Please insert server id");
    			return false;
    		}
        	if(Integer.parseInt(server) < 1 || Integer.parseInt(server) > mServerCount){
        		JOptionPane.showMessageDialog(null, "Server is not in range");
        		return false;
        	}
        	return true;
        }
        
        private boolean validateQuery(){
        	String query = queryBox.getText();
        	if(query.isEmpty()){
    			JOptionPane.showMessageDialog(null, "Please insert query");
    			return false;
    		}
        	return true;
        }
	}
	
	// purge button listener
	class PurgeButtonListener implements ActionListener { 
        public void actionPerformed(ActionEvent e) {
        	if(JOptionPane.showConfirmDialog(null, "Are you sure to purge distributed hash table?", "Purge",
        			JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION){
        		sendPurgeRequest();
        	}
        }
	}
	
	private void createAndShowGUI() {
		initComponents();
		initServers();
    }

	public static void main(String[] args) {
		String clientSettingFile = "";
		GetOpt getopt = new GetOpt(args, "f:r:");
		try {
			int c;
			while ((c = getopt.getNextOption()) != -1) {
			    switch(c) {
			    case 'f':
			    	clientSettingFile = getopt.getOptionArg();
			        break;
			    case 'r':
			    	String s = getopt.getOptionArg();
	            	DebugMode = (s.equals("debug"));
	                break;
	                
			    }
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		final DHTInteractiveClient dhtClient = new DHTInteractiveClient(clientSettingFile);
				
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
            	dhtClient.createAndShowGUI();
            }
        });

	}
}
