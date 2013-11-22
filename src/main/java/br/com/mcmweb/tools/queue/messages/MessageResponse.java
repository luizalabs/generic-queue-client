package br.com.mcmweb.tools.queue.messages;

public class MessageResponse {

	protected String id;
	protected String handle;
	protected String type;
	protected Object object;
	protected Long firstReceiptTimestamp = (long) 0;
	protected Boolean isRedeliver = false;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getHandle() {
		return handle;
	}

	public void setHandle(String handle) {
		this.handle = handle;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public Long getFirstReceiptTimestamp() {
		return firstReceiptTimestamp;
	}

	public void setFirstReceiptTimestamp(Long timestamp) {
		this.firstReceiptTimestamp = timestamp;
	}

	public Boolean getIsRedeliver() {
		return isRedeliver;
	}

	public void setIsRedeliver(Boolean isRedeliver) {
		this.isRedeliver = isRedeliver;
	}

}