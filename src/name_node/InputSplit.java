package name_node;

public class InputSplit{
	
	public enum STATUS {
		UNASSIGNED, RUNNING, FINISHED;
    }
	
	private long start;
	private long end;
	private STATUS status;
	
	public InputSplit(long start, long end) {
		this.start = start;
		this.end = end;
		this.status = STATUS.UNASSIGNED;
	}
	
	public void setRunning() {
		this.status = STATUS.RUNNING;
	}
}