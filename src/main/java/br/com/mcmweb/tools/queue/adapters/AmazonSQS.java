package br.com.mcmweb.tools.queue.adapters;

public class AmazonSQS extends GenericQueue {

	/**
	 * Initialize queue connections
	 * 
	 * @param host
	 *            should be the ARN
	 * @param login
	 *            should be the access key id
	 * @param password
	 *            should be the secret access key
	 * @param name
	 *            queue name
	 * @throws Exception
	 */
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
