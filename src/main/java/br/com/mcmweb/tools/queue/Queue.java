package br.com.mcmweb.tools.queue;

import java.lang.reflect.Constructor;
import java.util.logging.Logger;

import br.com.mcmweb.tools.queue.adapters.GenericQueue;
import br.com.mcmweb.tools.queue.adapters.QueueType;

public class Queue {

	private static final Logger logger = Logger.getLogger(Queue.class.getName());

    public static GenericQueue getInstance(String queueTypeName, String host, String login, String password, String queueName) {
        QueueType queueType = QueueType.valueOf(queueTypeName.toUpperCase());
            return Queue.getInstanceByType(queueType, host, login, password, queueName);
	}

	public static GenericQueue getInstanceByType(QueueType queueType, String host, String login, String password, String queueName) {
		try {
			Constructor<? extends GenericQueue> constructor = queueType.clazz().getDeclaredConstructor(String.class, String.class, String.class, String.class);
			return constructor.newInstance(host, login, password, queueName);
		} catch (Exception e) {
			logger.severe("Unable to instantiate queue " + queueType.toString() + ". Root cause: " + e.getMessage());
		}
		return null;
	}
}
