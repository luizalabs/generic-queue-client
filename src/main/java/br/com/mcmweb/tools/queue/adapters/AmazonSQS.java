package br.com.mcmweb.tools.queue.adapters;

public class AmazonSQS extends GenericQueue {

	public AmazonSQS(String host, String login, String password, String name) throws Exception {
		super(host, login, password, name);
	}

	@Override
	public void open() throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public String put(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean delete(String jobId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
