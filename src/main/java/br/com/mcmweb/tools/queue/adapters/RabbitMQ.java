package br.com.mcmweb.tools.queue.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQ extends GenericQueue {

	private Connection connection;
	private Channel channel;
	private boolean isQueueDeclared = false;
	private QueueingConsumer consumer;
	private Map<Long, Long> delayedList = new HashMap<Long, Long>();

	private static final Logger logger = Logger.getLogger(RabbitMQ.class.getName());

	public RabbitMQ(String host, String login, String password, String queueName) throws Exception {
		super(host, login, password, queueName);
		this.connect();
	}

	@Override
	public void connect() throws Exception {
		String[] hostParts = this.getHost().split(":");

		ConnectionFactory factory = new ConnectionFactory();
		Address[] addrArr = new Address[] { new Address(hostParts[0], Integer.parseInt(hostParts[1])) };
		this.connection = factory.newConnection(addrArr);
		this.channel = this.connection.createChannel();
	}

	/**
	 * Try to reconnect 5 times
	 * 
	 * TODO config?
	 * 
	 * @return
	 */
	private boolean reconnect() {
		int tries = 0;
		this.close();
		do {
			try {
				this.connect();
				logger.info("Queue connection is up again.");
				this.isQueueDeclared = false;
				this.queueDeclare();
				this.consumer = null;
				return true;
			} catch (Exception e1) {
				tries++;
				logger.severe("Unable to reach queue! Reason: " + e1.getMessage());
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		} while (tries < 5);
		return false;
	}

	private void queueDeclare() {
		if (!this.isQueueDeclared) {
			try {
				channel.queueDeclare(this.queueName, true, false, false, null);
				this.isQueueDeclared = true;
			} catch (IOException e) {
				logger.severe("Unable to declare queue " + this.queueName);
				logger.severe(e.getMessage());
			}
		}
	}

	private void enableConsumer() {
		this.queueDeclare();
		if (this.consumer == null) {
			this.consumer = new QueueingConsumer(channel);
			try {
				channel.basicConsume(this.queueName, false, this.consumer);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean put(Object object) {
		this.queueDeclare();
		try {
			this.channel.basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
			return true;
		} catch (AlreadyClosedException e) {
			logger.info("Queue channel is closed! Trying to reconnect. Reason: " + e);
			this.reconnect();
			try {
				this.channel.basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
				return true;
			} catch (Exception e1) {
				logger.severe("Unable to add to queue: " + e);
			}
		} catch (IOException e) {
			logger.info("Queue connection is down! Trying to reconnect. Reason: " + e);
			this.reconnect();
			try {
				this.channel.basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
				return true;
			} catch (Exception e1) {
				logger.severe("Unable to add to queue: " + e);
			}
		}
		return false;
	}

	@Override
	public MessageResponse getNext() {
		this.enableConsumer();
		this.releaseDelayed();
		try {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery(20000);
			if (delivery != null) {
				String id = Long.toString(delivery.getEnvelope().getDeliveryTag());
				String handle = id;

				int receivedCount;
				if (delivery.getEnvelope().isRedeliver()) {
					receivedCount = 666;
				} else {
					receivedCount = 0;
				}

				MessageResponse response = this.unserializeMessageBody(id, handle, receivedCount, new String(delivery.getBody()));
				return response;
			}
		} catch (AlreadyClosedException e) {
			logger.info("Queue channel is closed! Trying to reconnect. Reason: " + e);
			this.reconnect();
		} catch (ShutdownSignalException e) {
			logger.info("Queue connection is down! Trying to reconnect. Reason: " + e);
			this.reconnect();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				logger.severe("Unable to delete message. Reason: " + e);
				if (this.reconnect()) {
					try {
						channel.basicAck(messageId, false);
						return true;
					} catch (Exception e1) {
						logger.severe("Unable to delete message. Reason: " + e1);
					}
				}
				return true;
			} catch (IOException e) {
				logger.severe("Unable to delete message. Reason: " + e);
				if (this.reconnect()) {
					try {
						channel.basicAck(messageId, false);
						return true;
					} catch (Exception e1) {
						logger.severe("Unable to delete message. Reason: " + e1);
					}
				}
				return true;
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
				} catch (IOException e) {
					logger.severe("Unable to release message. Reason: " + e);
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
				} catch (IOException e) {
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
	public void close() {
		try {
			this.channel.close();
		} catch (Exception e) {
			logger.info("Unable to close channel: " + e.getMessage());
		}
		try {
			this.connection.close();
		} catch (Exception e) {
			logger.info("Unable to close connection: " + e.getMessage());
		}
	}

}