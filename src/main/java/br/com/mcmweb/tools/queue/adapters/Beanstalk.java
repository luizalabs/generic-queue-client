package br.com.mcmweb.tools.queue.adapters;

import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;

public class Beanstalk extends GenericQueue {

	private static final int DEFAULT_PRIORITY = 2048;
	private ClientImpl beanstalk;
	private static final Logger logger = Logger.getLogger(Beanstalk.class.getName());

	private static final ThreadLocal<Boolean> isTubeSelected = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};

	public Beanstalk(String host, String login, String password, String tubeName) throws Exception {
		super(host, login, password, tubeName);
	}

	@Override
	public void connect() throws Exception {
		isTubeSelected.set(false);
		String[] hostParts = this.getHost().split(":");
		this.beanstalk = new ClientImpl(hostParts[0], Integer.parseInt(hostParts[1]));
	}

	@Override
	public boolean reconnect() {
		int retries = 0;
		do {
			try {
				this.connect();
				logger.info("Queue connection is up again.");
				return true;
			} catch (Exception ce) {
				logger.info("Unable to reconnect Beanstalk: " + ce);
			}
			reconnectSleepTimer();
			retries++;
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	private void defineTubeConnection() {
		if (!isTubeSelected.get()) {
			this.beanstalk.useTube(this.queueName);
			this.beanstalk.watch(this.queueName);
			isTubeSelected.set(true);
		}
	}

	@Override
	public boolean put(Object object) {
		try {
			this.defineTubeConnection();
			long id = this.beanstalk.put(DEFAULT_PRIORITY, 0, 300, this.serializeMessageBody(object).getBytes());
			logger.finest("Added Beanstalk message, id #" + id);
			return true;
		} catch (BeanstalkException e) {
			if (this.reconnect()) {
				return this.put(object);
			}
			logger.severe("Error adding Beanstalk message: " + e);
		} catch (Exception e) {
			logger.severe("Unknown error adding Beanstalk message: " + e);
		}
		return false;
	}

	@Override
	public MessageResponse getNext() {
		try {
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
					logger.warning("Unable to determine received count: " + e);
				}

				MessageResponse response = this.unserializeMessageBody(id, handle, receivedCount, new String(job.getData()));
				return response;
			}
		} catch (BeanstalkException e) {
			this.reconnect();
		} catch (Exception e) {
			logger.severe("Unknown error reading Beanstalk message: " + e);
		}
		return null;
	}

	@Override
	public boolean delete(MessageResponse message) {
		try {
			this.defineTubeConnection();
			boolean status = this.beanstalk.delete(Long.parseLong(message.getHandle()));
			return status;
		} catch (BeanstalkException e) {
			if (this.reconnect()) {
				return this.delete(message);
			}
			logger.severe("Error reading Beanstalk message: " + e);
		} catch (Exception e) {
			logger.severe("Unknown error reading Beanstalk message: " + e);
		}
		return false;
	}

	@Override
	public boolean release(MessageResponse message, Integer delaySeconds) {
		if (delaySeconds == null) {
			delaySeconds = 0;
		}
		try {
			this.defineTubeConnection();
			boolean status = this.beanstalk.release(Long.parseLong(message.getHandle()), DEFAULT_PRIORITY, delaySeconds);
			return status;
		} catch (BeanstalkException e) {
			if (this.reconnect()) {
				return this.release(message, delaySeconds);
			}
			logger.severe("Error releasing Beanstalk message: " + e);
		} catch (Exception e) {
			logger.severe("Unknown error releasing Beanstalk message: " + e);
		}
		return false;
	}

	@Override
	public boolean touch(MessageResponse message) {
		try {
			this.defineTubeConnection();
			boolean status = this.beanstalk.touch(Long.parseLong(message.getHandle()));
			return status;
		} catch (BeanstalkException e) {
			if (this.reconnect()) {
				return this.touch(message);
			}
			logger.severe("Error touching Beanstalk message: " + e);
		} catch (Exception e) {
			logger.severe("Unknown error touching Beanstalk message: " + e);
		}
		return false;
	}

	@Override
	@PreDestroy
	public void close() {
		if (this.beanstalk != null) {
			try {
				this.beanstalk.close();
			} catch (Exception e) {
				logger.info("Unable to close Beanstalk connection: " + e);
			}
		}
	}

}