package br.com.mcmweb.tools.queue.adapters;

import java.util.Map;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;

public class Beanstalk extends GenericQueue {

	private static final int DEFAULT_PRIORITY = 2048;
	private ClientImpl beanstalk;

	private static final ThreadLocal<Boolean> isTubeSelected = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};

	public Beanstalk(String host, String login, String password, String tubeName) throws Exception {
		super(host, login, password, tubeName);
		this.connect();
	}

	@Override
	public void connect() throws Exception {
		isTubeSelected.set(false);
		String[] hostParts = this.getHost().split(":");
		this.beanstalk = new ClientImpl(hostParts[0], Integer.parseInt(hostParts[1]));
	}

	private void defineTubeConnection() {
		if (!isTubeSelected.get()) {
			this.beanstalk.useTube(this.queueName);
			this.beanstalk.watch(this.queueName);
			isTubeSelected.set(true);
		}
	}

	@Override
	public String put(Object object) {
		this.defineTubeConnection();
		// TODO conf or parameter
		long id = this.beanstalk.put(DEFAULT_PRIORITY, 0, 300, this.serializeMessageBody(object).getBytes());
		return Long.toHexString(id);
	}

	@Override
	public MessageResponse getNext() {
		this.defineTubeConnection();
		Job job = this.beanstalk.reserve(20); // TODO config
		if (job != null) {
			String id = Long.toHexString(job.getJobId());
			String handle = Long.toString(job.getJobId());

			Map<String, String> stats = this.beanstalk.statsJob(job.getJobId());

			Integer receivedCount = null;
			try {
				receivedCount = Integer.parseInt(stats.get("reserves")) - 1;
			} catch (Exception e) {
				// TODO log warning
			}

			MessageResponse response = this.unserializeMessageBody(id, handle, receivedCount, new String(job.getData()));
			return response;
		}
		return null;
	}

	@Override
	public Boolean delete(MessageResponse message) {
		this.defineTubeConnection();
		return this.beanstalk.delete(Long.parseLong(message.getHandle()));
	}

	@Override
	public Boolean release(MessageResponse message, Integer delaySeconds) {
		if (delaySeconds == null) {
			delaySeconds = 0;
		}
		this.defineTubeConnection();
		return this.beanstalk.release(Long.parseLong(message.getHandle()), DEFAULT_PRIORITY, delaySeconds);
	}

	@Override
	public Boolean touch(MessageResponse message) {
		this.defineTubeConnection();
		return this.beanstalk.touch(Long.parseLong(message.getHandle()));
	}

	@Override
	public void close() {
		this.beanstalk.close();
	}

}