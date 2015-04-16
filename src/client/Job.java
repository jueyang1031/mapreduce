package client;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import client.Mapper;
import context.JobContext;

public class Job{

	private JobContext jobContext;
	private String masterName;
	private int masterPort;
	
	public void setMapperClass(Class<? extends Mapper> cls
			) throws IllegalStateException {
		jobContext.setMapperClass(cls);
	}
	
	public void setReducerClass(Class<? extends Mapper> cls
			) throws IllegalStateException {
		jobContext.setReducerClass(cls);
	}
	
	public void setInputPath(String inputPath
			) throws IllegalStateException {
		jobContext.setInputPath(inputPath);
	}
	
	public void waitForCompletion() throws IOException {
		//TODO: Get masterName and masterPort from S3
		
		Socket socket = new Socket(this.masterName, this.masterPort);
		
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		out.writeUTF("Client");
		
		ObjectOutputStream outObj = new ObjectOutputStream(socket.getOutputStream());
		outObj.writeObject(this.jobContext);
		//hahaha
	}
}
