package data_node;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;

import sss.SSS;
import context.JobContext;
import name_node.InputSplit;

public class MyTaskTracker {

	private String serverName;
	private int serverPort;
	private boolean isWaiting;
	private JobContext jobContext;
	private InputSplit inputSplit;
	private String mapperOut;

	public MyTaskTracker() {
		this.isWaiting = true;
	}

	public void run() {

		try {
			// TODO: Get masterName and masterPort from S3

			Socket socket = new Socket(this.serverName, this.serverPort);
			DataOutputStream out = new DataOutputStream(
					socket.getOutputStream());
			out.writeUTF("TaskTracker");
			while (isWaiting) {
			}

			ObjectInputStream inObj = new ObjectInputStream(
					socket.getInputStream());
			this.jobContext = (JobContext) inObj.readObject();
			this.inputSplit = (InputSplit) inObj.readObject();

			// TODO: download file from s3
			SSS s3 = new SSS();
			s3.getBucketNameAndKey(jobContext.getJarPath());
			InputStream inputStream = s3.downloadRange(inputSplit.getStart(),
					inputSplit.getEnd());
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(inputStream, "UTF-8"));
			// for each line execute map
			while (bufferedReader.readLine() != null) {
				// TODO: Load class and execute map
				this.mapperOut += loadClass(bufferedReader.readLine(), "map");
			}

			// HeartBeat Handling
			while (true) {
				String outStr = "heartbeat from client: "
						+ socket.getLocalSocketAddress();
				out.writeUTF(outStr);
				Thread.sleep(1000);
			}

			// serverClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void awake() {
		isWaiting = false;
	}

	public static void main(String[] args) {
		MyTaskTracker taskTracker = new MyTaskTracker();
		taskTracker.run();
	}

	public String loadClass(String lineIn, String methodName) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException, SecurityException,
			NoSuchMethodException, IllegalArgumentException,
			InvocationTargetException, IOException, InterruptedException {
		URL[] urls = new URL[1];
		urls[0] = new URL(jobContext.getJarPath());
		URLClassLoader uRLClassLoader = new URLClassLoader(urls, this
				.getClass().getClassLoader());
		Class classToLoad = Class.forName(jobContext.getMapperName(), true,
				uRLClassLoader);
		Object instancer = classToLoad.newInstance();
		Method method = classToLoad.getMethod(methodName, String.class,
				String.class, String.class, String.class);
		String keyValue = (String) method.invoke(instancer, lineIn);
		return keyValue;
	}
}
