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

	private void declareQueue() {
		if (!this.isQueueDeclared) {
			try {
				channel.queueDeclare(this.queueName, false, false, false, null);
				this.isQueueDeclared = true;
			} catch (IOException e) {
				logger.severe("Unable to declare queue " + this.queueName);
				logger.severe(e.getMessage());
			}
		}
	}

	private void enableConsumer() {
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
		this.declareQueue();
		try {
			this.channel.basicPublish("", this.queueName, null, this.serializeMessageBody(object).getBytes());
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			channel.basicAck(Long.parseLong(message.getId()), false);
			return true;
		} catch (NumberFormatException e) {
			logger.severe("Unable to parse message id " + message.getId());
		} catch (IOException e) {
			logger.severe("Unable to delete message. Reason: " + e);
		}
		return false;
	}

	@Override
	public boolean release(MessageResponse message, Integer delaySeconds) {
		try {
			long deliveryTag = Long.parseLong(message.getId());
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
			logger.severe("Unable to parse message id " + message.getId());
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}