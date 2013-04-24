package br.com.mcmweb.tools.queue.adapters;

public abstract class GenericQueue {

	private String host;
	private String login;
	private String password;
	private String queueName;

	public GenericQueue(String host, String login, String password, String queueName) throws Exception {
		this.host = host;
		this.login = login;
		this.password = password;
		this.queueName = queueName;
	}

	public abstract void open() throws Exception;

	public abstract String put(Object object);

	public abstract boolean delete(String jobId);

	public abstract void close();

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

}
