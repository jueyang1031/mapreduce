package context;

import client.Mapper;

public class JobContext{
	
	private String mapperName;
	private String reducerName;
	private String inputPath;
	private String jarPath;
	
	public String getMapperName() {
		return mapperName;
	}

	public void setMapperName(String mapperName) {
		this.mapperName = mapperName;
	}

	public String getReducerName() {
		return reducerName;
	}

	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}

	public String getInputPath() {
		return inputPath;
	}

	
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

	public String getJarPath() {
		return jarPath;
	}

	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	
}