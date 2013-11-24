package br.com.mcmweb.tools.queue.messages;

import org.joda.time.DateTime;

public class MessageRequest {

	protected String type;
	protected String body;
	protected DateTime creationDate = null;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}
	
	public DateTime getCreationDate () {
		return this.creationDate;
	}
	
	public void setCreationDate (DateTime creationDate) {
		this.creationDate = creationDate;
	}

}