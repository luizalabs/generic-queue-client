package br.com.mcmweb.tools.queue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import br.com.mcmweb.tools.queue.adapters.GenericQueue;
import br.com.mcmweb.tools.queue.adapters.QueueType;

public class Queue {

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

	public static void main(String[] args) {
		List<String> hosts = new ArrayList<String>();
		hosts.add("127.0.0.1");
		GenericQueue x = Queue.getInstance(QueueType.BEANSTALKD, "127.0.0.1:1234", null, null, "myqueue");
		x.put(hosts);
	}

}
