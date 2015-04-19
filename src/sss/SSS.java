package sss;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class SSS{
	
	private String bucketName;
	private String key;
	private AWSCredentials credentials;
	private AmazonS3 s3Client;
	
	public SSS() {
		this.credentials = new BasicAWSCredentials(
				"AKIAJ5WJ4YUIP4GWFXPA",
				"OZnc0nwMvteIRDjmfhuOjFvZd5B3ckMQJ1kG7zmR");
		this.s3Client = new AmazonS3Client(credentials);
	}
	
	public void getBucketNameAndKey(String jarPath){
		String[] dirs = jarPath.split("/");
		this.bucketName = dirs[3];
		int length = dirs[0].length() + dirs[1].length() 
				+ dirs[2].length() + dirs[3].length() + 4;
		this.key = jarPath.substring(length);
	}
	
	public InputStream downloadAll() throws IOException {
		S3Object object = s3Client.getObject(
                new GetObjectRequest(bucketName, key));
		InputStream objectData = object.getObjectContent();
		//Process the objectData stream.
		objectData.close();
		return objectData;
	}
	
	public long getFileSize() {
		S3Object object = s3Client.getObject(
                new GetObjectRequest(bucketName, key));
		return object.getObjectMetadata().getContentLength();
	}
	
	public InputStream downloadRange(long start, long end) {
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(
				bucketName, key);
		rangeObjectRequest.setRange(start, end); // retrieve 1st 10 bytes.
		S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

		InputStream objectData = objectPortion.getObjectContent();
		return objectData;
	}
}