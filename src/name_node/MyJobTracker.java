package name_node;
import java.util.List;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import sss.SSS;

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
	private static final int blockSize = 16777216;
	private static final int maxLineSize = 100;
	
//	public MyJobTracker(int port) throws IOException {
//		super(); 
//		this.myThreads = new ArrayList<MyThread>();
//		this.inputSplits = new ArrayList<InputSplit>();
//		this.serverSocket = new ServerSocket(port);
//		this.jobContext = new JobContext();
//		//serverSocket.setSoTimeout(10000);
//	}
	
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
	
//	public static void main(String[] args)
//	{
//		int port = Integer.parseInt(args[0]);
//	    try {
//	    	MyJobTracker jobtracker = new MyJobTracker(port);
//	    	jobtracker.run();
//	    } catch(IOException e) {
//	        e.printStackTrace();
//	    }
//	}
	
	@Test
	public void getInputSplit() throws IOException {
		SSS s3 = new SSS();
		s3.getBucketNameAndKey("https://s3-us-west-2.amazonaws.com/juemedian/input/purchases4.txt");
		//InputStream inputStream = s3.downloadRange(0, blockSize);
		long fileSize = s3.getFileSize();
		int i = 1;
		long start = 0;
		long end = 0;
		ArrayList<InputSplit> inputSplits = new ArrayList<InputSplit>();
		while((start + blockSize) <= fileSize) {
			InputSplit inputSplit = new InputSplit();
			InputStream inputStream;
			inputSplit.setStart(start);
			if ((start + blockSize + maxLineSize) <= fileSize) {
				inputStream = s3.downloadRange(start + blockSize, start + blockSize + maxLineSize);
				String beyond = IOUtils.toString(inputStream, "UTF-8");
				String[] splits = beyond.split("\n");
				end = start + blockSize + splits[0].length();
			}else {
				inputStream = s3.downloadRange(start + blockSize, fileSize);
				end = fileSize;
			}
			inputSplit.setEnd(end);
			start = end + 1;
			inputSplits.add(inputSplit);
			//++i;
		}
		for(InputSplit inputSplit : inputSplits) {
			InputStream inputStream = s3.downloadRange(inputSplit.getStart(), inputSplit.getEnd());
			String beyond = IOUtils.toString(inputStream, "UTF-8");
			System.out.println(beyond);
		}
	}

	public MyJobTracker() {
		super();
	}
}