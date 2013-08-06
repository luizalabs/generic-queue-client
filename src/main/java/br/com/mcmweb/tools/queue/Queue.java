package br.com.mcmweb.tools.queue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import br.com.mcmweb.tools.queue.adapters.GenericQueue;
import br.com.mcmweb.tools.queue.adapters.QueueType;

public class Queue {
	
	public static GenericQueue getInstance(String queueTypeName, String host, String login, String password, String queueName) {
		QueueType queueType = QueueType.valueOf(queueTypeName.toUpperCase());
		return Queue.getInstance(queueType, host, login, password, queueName);
	}

	public static GenericQueue getInstance(QueueType queueType, String host, String login, String password, String queueName) {
		try {
			Constructor<? extends GenericQueue> constructor = queueType.clazz().getDeclaredConstructor(String.class, String.class, String.class, String.class);
			return constructor.newInstance(host, login, password, queueName);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
