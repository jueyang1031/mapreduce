package name_node;

public class InputSplit{
	
	public InputSplit() {
		super();
	}

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

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}
}