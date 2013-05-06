package br.com.mcmweb.tools.queue.messages;

public class MessageRequest {

	protected String type;
	protected String body;

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

}
