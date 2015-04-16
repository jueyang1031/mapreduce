package name_node;
import java.util.List;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import context.JobContext;

public class MyJobTracker extends Thread {
	
	private ArrayList<MyThread> myThreads;
	private ArrayList<InputSplit> inputSplits;
	private ServerSocket serverSocket;
	private JobContext jobContext;

	public MyJobTracker(int port) throws IOException {
		super(); 
		this.myThreads = new ArrayList<MyThread>();
		this.inputSplits = new ArrayList<InputSplit>();
		this.serverSocket = new ServerSocket(port);
		this.jobContext = new JobContext();
		//serverSocket.setSoTimeout(10000);
	}
	
	public void run() {
		
		while(true) {
	         try {
	        	 
	        	 //Upload IP and Port onto S3
	        	 String ip = serverSocket.getInetAddress().toString();
	        	 
	        	 Socket socket = serverSocket.accept();
	        	 
	        	 //Identify client or tasktracker
	        	 DataInputStream in = new DataInputStream(socket.getInputStream());
	        	 String msg = in.readUTF();
	        	 
	        	 if(msg.equals("TaskTracker")) {
	        		 MyThread clientThread = new MyThread(socket);
	        		 myThreads.add(clientThread);
	        	 } else if(msg.equals("Client")) {
	        		 ObjectInputStream inObj = new ObjectInputStream(socket.getInputStream());
	        		 this.jobContext = (JobContext)inObj.readObject();
	        		 
	        		 //Construct list of InputSplits  How to break?
	        		 
	        		 
	        		 for (int i = 0; i < myThreads.size(); i++) {
	        			 
	        			 //Find first unassigned InputSplit
	        			 
	        			 
	        			 //myThreads.get(i).startWorking(this.jobContext, /* InputSplit */);
	        		 }
	        	 }
	        	 
			     
			     //clientThread.run();
	         } catch(SocketTimeoutException s) {
	            System.out.println("Socket timed out!");
	            break;
	         } catch(IOException e) {
	            e.printStackTrace();
	            break;
	         }catch(ClassNotFoundException e) {
		        e.printStackTrace();
		        break;
		     }
	    }		
	}
	
	public void testS3() {
		AWSCredentials credentials = new BasicAWSCredentials("AKIAJDSEM7P344JDBAEQ", "79AOuVMffkVI4ALF5yVoNp1bplgUx/0bzcLRy28k");
		AmazonS3 s3client = new AmazonS3Client(credentials);

		String bucketName = "willard";
		String fileName = "Test/MasterIPPort.txt";
		String str = "Upload";
		List<Bucket> buckets = s3client.listBuckets();
		s3client.createBucket("omg");
		s3client.putObject(new PutObjectRequest(bucketName, fileName, str));
	}
	
	public static void main(String[] args)
	{
		int port = Integer.parseInt(args[0]);
	    try {
	    	MyJobTracker jobtracker = new MyJobTracker(port);
	    	jobtracker.run();
	    } catch(IOException e) {
	        e.printStackTrace();
	    }
	}
}