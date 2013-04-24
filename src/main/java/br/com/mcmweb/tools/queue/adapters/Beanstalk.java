package br.com.mcmweb.tools.queue.adapters;

import com.surftools.BeanstalkClientImpl.ClientImpl;

public class Beanstalk extends GenericQueue {

	private ClientImpl beanstalk;

	public Beanstalk(String host, String login, String password, String name) throws Exception {
		super(host, login, password, name);
		this.open();
	}

	@Override
	public void open() throws Exception {
		String[] hostParts = this.getHost().split(":");
		this.beanstalk = new ClientImpl(hostParts[0], Integer.parseInt(hostParts[1]));
	}

	@Override
	public String put(Object object) {
//		this.beanstalk.put(priority, delaySeconds, timeToRun, data);
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
