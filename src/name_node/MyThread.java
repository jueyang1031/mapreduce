package name_node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import context.JobContext;

public class MyThread extends Thread {
    
	protected Socket socket;
	private long lastUpdateTime;
	private long timeout;
	private JobContext jobContext;
	private InputSplit inputSplit;

    public MyThread(Socket clientSocket) {
        this.socket = clientSocket;
    }

    public void run() {
        try {
        	DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            
            String outStr = "Hello from server " + socket.getLocalSocketAddress();
            out.writeUTF(outStr);
            
            while (true) {
            	if (in.readUTF() != null) {
            		this.lastUpdateTime = System.currentTimeMillis();
            		System.out.println("Server receives: " + in.readUTF());
            	}
            }	
        } catch(IOException e) {
            e.printStackTrace();
        } 
    }
    
    public boolean isHBTimeout() {
		long now = System.currentTimeMillis();
		return (now > this.lastUpdateTime + this.timeout);
	}
    
    public void startWorking(JobContext jobContext, InputSplit inputSplit) {
    	this.jobContext = jobContext;
    	this.inputSplit = inputSplit;
    	run();
    }
}