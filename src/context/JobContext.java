package context;

import client.Mapper;

public class JobContext{
	
	private String mapperName;
	private String reducerName;
	private String inputPath;
	
	public void setMapperClass(Class<? extends Mapper> cls
			) throws IllegalStateException {
		mapperName = cls.getName();
	}
	
	public void setReducerClass(Class<? extends Mapper> cls
			) throws IllegalStateException {
		reducerName = cls.getName();
	}
	
	public void setInputPath(String inputPath
			) throws IllegalStateException {
		this.inputPath = inputPath;
	}
	
}