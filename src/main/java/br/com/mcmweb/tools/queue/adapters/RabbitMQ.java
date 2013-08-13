package br.com.mcmweb.tools.queue.adapters;

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
	private Channel channel;
	private QueueingConsumer consumer;
	private boolean isConsumer = false;
	private Map<Long, Long> delayedList = new HashMap<Long, Long>();

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
		this.channel = this.connection.createChannel();
		this.channel.basicQos(1);
		this.channel.queueDeclare(this.queueName, true, false, false, null);
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
			this.close();
			try {
				this.connect();
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

	private void consumerSetup() {
		if (!this.isConsumer) {
			try {
				QueueingConsumer newConsumer = new QueueingConsumer(channel);
				channel.basicConsume(this.queueName, false, newConsumer);
				this.consumer = newConsumer;
				this.isConsumer = true;
			} catch (Exception e) {
				logger.severe("Unable to start consuming queue. Reason: " + e);
			}
		}
	}

	@Override
	public boolean put(Object object) {
		try {
			channel.basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
			logger.finest("Added RabbitMQ message");
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
			QueueingConsumer.Delivery delivery = consumer.nextDelivery(20000);
			if (delivery != null) {
				String id = Long.toString(delivery.getEnvelope().getDeliveryTag());
				String handle = id;

				int receivedCount;
				if (delivery.getEnvelope().isRedeliver()) {
					receivedCount = 666; // FIXME :(
				} else {
					receivedCount = 0;
				}

				MessageResponse response = this.unserializeMessageBody(id, handle, receivedCount, new String(delivery.getBody()));
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
		}
		return null;
	}

	@Override
	public boolean delete(MessageResponse message) {
		try {
			long messageId = Long.parseLong(message.getHandle());
			try {
				channel.basicAck(messageId, false);
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
					channel.basicNack(deliveryTag, false, true);
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
				delayedList.put(deliveryTag, System.currentTimeMillis() + (delaySeconds * 1000));
			}
		} catch (NumberFormatException e) {
			logger.severe("Unable to parse message id " + message.getHandle());
		}
		return false;
	}

	private synchronized void releaseDelayed() {
		long now = System.currentTimeMillis();
		List<Long> removedList = new ArrayList<Long>();
		for (Entry<Long, Long> delayed : delayedList.entrySet()) {
			if (delayed.getValue() <= now) {
				try {
					channel.basicNack(delayed.getKey(), false, true);
					removedList.add(delayed.getKey());
				} catch (Exception e) {
					logger.severe("Unable to release message after " + (now - delayed.getValue()) + " ms. Reason: " + e);
				}
			}
		}
		for (Long removedDeliveryTag : removedList) {
			delayedList.remove(removedDeliveryTag);
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
			this.isConsumer = false;
		}
	}

}