package br.com.mcmweb.tools.queue.adapters;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQ extends GenericQueue {

	private Connection connection;
	private ThreadLocal<Channel> channel = new ThreadLocal<Channel>();
	private ThreadLocal<QueueingConsumer> consumer = new ThreadLocal<QueueingConsumer>();
	private ThreadLocal<Boolean> isConsumer = new ThreadLocal<Boolean>();
	private ThreadLocal<Map<Long, Long>> delayedList = new ThreadLocal<Map<Long, Long>>();
	private static final Logger logger = Logger.getLogger(RabbitMQ.class.getName());

	public RabbitMQ(String host, String login, String password, String queueName) throws Exception {
		super(host, login, password, queueName);
	}

	@Override
	public void connect() throws Exception {
		String[] hostParts = this.getHost().split(":"); // TODO constructor

		ConnectionFactory factory = new ConnectionFactory();
		Address[] addrArr = new Address[] { new Address(hostParts[0], Integer.parseInt(hostParts[1])) };

		this.connection = factory.newConnection(addrArr);
	}

	/**
	 * Try to reconnect
	 * 
	 * @return
	 */
	@Override
	protected boolean reconnect() {
		int retries = 0;
		do {
			try {
				if (this.connection == null || !this.connection.isOpen()) {
					this.close();
					this.connect();
				} else {
					this.closeChannel();
				}
				logger.info("Queue connection is up again.");
				return true;
			} catch (ConnectException ce) {
				logger.warning("Unable to reconnect RabbitMQ: " + ce);
			} catch (Exception e) {
				logger.warning("Unable to reconnect RabbitMQ: " + e);
				break;
			}
			reconnectSleepTimer();
			retries++;
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	private Channel getChannel() {
		Channel channel = null;
		try {
			if (this.connection == null) {
				this.connect();
			}
			if (this.channel.get() == null) {
				channel = this.connection.createChannel();
				channel.basicQos(1);
				channel.queueDeclare(this.queueName, true, false, false, null);
				this.channel.set(channel);
			} else {
				channel = this.channel.get();
			}
		} catch (IOException e) {
			logger.severe("Unable to get queue channel. Reason: " + e);
			this.reconnect();
		} catch (Exception e) {
			logger.severe("Unable to connect to queue. Reason: " + e);
		}
		return channel;
	}

	private void consumerSetup() throws IOException {
		if (this.isConsumer == null || this.isConsumer.get() == null || !this.isConsumer.get()) {
			if (this.isConsumer == null) {
				this.isConsumer = new ThreadLocal<Boolean>();
			}
			if (this.consumer == null) {
				this.consumer = new ThreadLocal<QueueingConsumer>();
			}
			Channel channel = this.getChannel();
			QueueingConsumer newConsumer = new QueueingConsumer(channel);
			channel.basicConsume(this.queueName, false, newConsumer);
			this.consumer.set(newConsumer);
			this.isConsumer.set(true);
		}
	}

	@Override
	public boolean put(Object object) {
		try {
			this.getChannel().basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
			logger.finest("Added RabbitMQ message");
			return true;
		} catch (AlreadyClosedException e) {
			if (this.reconnect()) {
				return this.put(object);
			}
			logger.severe("Error adding RabbitMQ message: " + e);
		} catch (Exception e) {
			logger.severe("Unknown error adding RabbitMQ message: " + e);
		}
		return false;
	}

	@Override
	public MessageResponse getNext() {
		try {
			this.consumerSetup();
			this.releaseDelayed();
			QueueingConsumer.Delivery delivery = consumer.get().nextDelivery(20000);
			if (delivery != null) {
				String id = Long.toString(delivery.getEnvelope().getDeliveryTag());
				String handle = id;

				MessageResponse response = this.unserializeMessageBody(id, handle, new String(delivery.getBody()));
				return response;
			}
		} catch (InterruptedException e) {
			// do nothing, app server shutdown
		} catch (AlreadyClosedException e) {
			this.reconnect();
		} catch (ShutdownSignalException e) {
			this.reconnect();
		} catch (Exception e) {
			logger.severe("Unknown error reading RabbitMQ message: " + e);
			this.reconnectSleepTimer();
		}
		return null;
	}

	@Override
	public boolean delete(MessageResponse message) {
		try {
			long messageId = Long.parseLong(message.getHandle());
			try {
				this.getChannel().basicAck(messageId, false);
				return true;
			} catch (AlreadyClosedException e) {
				if (this.reconnect()) {
					return this.delete(message);
				}
				logger.severe("Error deleting RabbitMQ message: " + e);
			} catch (Exception e) {
				logger.severe("Unknown error deleting RabbitMQ message: " + e);
			}
		} catch (NumberFormatException e) {
			logger.severe("Unable to parse message id " + message.getHandle());
		}
		return false;
	}

	@Override
	public boolean release(MessageResponse message, Integer delaySeconds) {
		try {
			long deliveryTag = Long.parseLong(message.getHandle());
			if (delaySeconds == null || delaySeconds == 0) {
				try {
					this.getChannel().basicNack(deliveryTag, false, true);
					return true;
				} catch (AlreadyClosedException e) {
					if (this.reconnect()) {
						return this.release(message, delaySeconds);
					}
					logger.severe("Error releasing RabbitMQ message: " + e);
				} catch (Exception e) {
					logger.severe("Unknown error releasing RabbitMQ message: " + e);
				}
			} else {
				Map<Long, Long> jobs = delayedList.get();
				if (jobs == null) {
					jobs = new HashMap<Long, Long>();
					jobs.put(deliveryTag, System.currentTimeMillis() + (delaySeconds * 1000));
					delayedList.set(jobs);
				} else {
					jobs.put(deliveryTag, System.currentTimeMillis() + (delaySeconds * 1000));
				}
			}
		} catch (NumberFormatException e) {
			logger.severe("Unable to parse message id " + message.getHandle());
		}
		return false;
	}

	private void releaseDelayed() {
		Map<Long, Long> jobs = delayedList.get();
		if (jobs != null) {
			long now = System.currentTimeMillis();
			List<Long> removedList = new ArrayList<Long>();
			for (Entry<Long, Long> job : jobs.entrySet()) {
				if (job.getValue() <= now) {
					try {
						this.getChannel().basicNack(job.getKey(), false, true);
						removedList.add(job.getKey());
					} catch (Exception e) {
						logger.severe("Unable to release message after " + (now - job.getValue()) + " ms. Reason: " + e);
					}
				}
			}
			for (Long removedDeliveryTag : removedList) {
				jobs.remove(removedDeliveryTag);
			}
		}
	}

	@Override
	public boolean touch(MessageResponse message) {
		// ignored by rabbitmq
		return true;
	}

	@Override
	@PreDestroy
	public void close() {
		if (this.connection != null) {
			this.connection.abort();
			try {
				this.closeChannel();
			} catch (IOException e) {
				// do nothing
			}
		}
	}

	public void closeChannel() throws IOException {
		if (this.channel != null) {
			Channel currentChannel = this.channel.get();
			if (currentChannel != null) {
				this.channel.set(null);
				currentChannel.close();
			}
		}
		if (this.consumer != null) {
			this.consumer.set(null);
		}
		if (this.isConsumer != null) {
			this.isConsumer.set(false);
		}
	}

}