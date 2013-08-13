package br.com.mcmweb.tools.queue.adapters;

import org.junit.Before;
import org.junit.Test;

import br.com.mcmweb.tools.queue.Queue;
import br.com.mcmweb.tools.queue.messages.MessageResponse;

public class BeanstalkTest {

	private GenericQueue adapter;

	@Before
	public void setup() {
		// String arn = "arn:aws:sqs:us-east-1:963891800096:ml-queue-test";
		String url = "localhost:11300";
		// String key = "AKIAI2FUC2V6LF3TUECA";
		// String secretKey = "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";
		String queueName = "ml-queue-test";

		this.adapter = Queue.getInstance(QueueType.BEANSTALKD, url, null, null, queueName);
	}

	// @Test
	// public void shouldAddObjectToQueue() {
	// QueueTest test = new QueueTest();
	// String id = this.adapter.put(test);
	// // assert(id != null);
	// // assert(id.length() > 0);
	// // assert(1 == 2);
	// System.out.println(id);
	// System.out.println(id.length());
	// }

	@Test
	public void shouldRetrieveObjectFromQueue() {
		// Add to queue
		QueueTest test = new QueueTest();
		Boolean success = this.adapter.put(test);

		MessageResponse response = this.adapter.getNext();
		if (response != null) {
			Object myObject = response.getObject();
			if (myObject == null) {
				System.out.println("veio não...");
			} else if (myObject instanceof QueueTest) {
				System.out.println("SHOWWW");
			} else {
				System.out.println("fudiô!");
			}
	
			// System.out.println(test);
			if (this.adapter.delete(response)) {
				System.out.println("Removeu de boa");
			} else {
				System.out.println("Falhou remoção");
			}
		}
	}

}
