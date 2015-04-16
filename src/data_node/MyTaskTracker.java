package data_node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import context.JobContext;
import name_node.InputSplit;

public class MyTaskTracker {
	
	private String serverName;
	private int serverPort;
	private boolean isWaiting; 
	private JobContext jobContext;
	private InputSplit inputSplit;
	
	public MyTaskTracker() {
		this.isWaiting = true;
	}
	
	public void run() {
		
		try {
			//TODO: Get masterName and masterPort from S3
			
			Socket socket = new Socket(this.serverName, this.serverPort);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			out.writeUTF("TaskTracker");
			while(isWaiting) {}

			ObjectInputStream inObj = new ObjectInputStream(socket.getInputStream());
			this.jobContext = (JobContext)inObj.readObject();
			this.inputSplit = (InputSplit)inObj.readObject();
			
			//TODO: Load class and execute map
			
			
			//HeartBeat Handling
			while(true) {
				String outStr = "heartbeat from client: " + socket.getLocalSocketAddress();
	        	out.writeUTF(outStr);
	        	Thread.sleep(1000);
			}
			
			//serverClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void awake() {
		isWaiting = false;
	}
	
	public static void main(String[] args)
	{
		MyTaskTracker taskTracker = new MyTaskTracker();
		taskTracker.run();
	}
}
