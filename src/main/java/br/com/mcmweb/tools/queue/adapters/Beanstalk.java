package br.com.mcmweb.tools.queue.adapters;

import java.util.ArrayList;
import java.util.List;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;

public class Beanstalk extends GenericQueue {

	private ClientImpl beanstalk;

	public Beanstalk(String host, String login, String password, String tubeName) throws Exception {
		super(host, login, password, tubeName);
		this.connect();
	}

	@Override
	public void connect() throws Exception {
		String[] hostParts = this.getHost().split(":");
		this.beanstalk = new ClientImpl(hostParts[0], Integer.parseInt(hostParts[1]));
		this.beanstalk.useTube(this.queueName);
		this.beanstalk.watch(this.queueName);
	}

	@Override
	public String put(Object object) {
		// TODO conf or parameter
		long id = this.beanstalk.put(2048, 0, 240, this.serializeMessageBody(object).getBytes());
		return Long.toHexString(id);
	}

//	@Override
//	public List<String> put(List<Object> objectList) {
//		List<String> status = new ArrayList<String>();
//		for (Object object : objectList) {
//			status.add(this.put(object));
//		}
//		return status;
//	}

	@Override
	public MessageResponse getNext() {
		Job job = this.beanstalk.reserve(20); // TODO config
		if (job != null) {
			String id = Long.toHexString(job.getJobId());
			String handle = Long.toString(job.getJobId());
			int receivedCount = 0;
			MessageResponse response = this.unserializeMessageBody(id, handle, receivedCount, new String(job.getData()));
			return response;
		}
		return null;
	}

	@Override
	public Boolean delete(MessageResponse message) {
		return this.beanstalk.delete(Long.parseLong(message.getHandle()));
	}

//	@Override
//	public List<Boolean> delete(List<MessageResponse> messageList) {
//		List<Boolean> status = new ArrayList<Boolean>();
//		for (MessageResponse message : messageList) {
//			status.add(this.delete(message));
//		}
//		return status;
//	}

	@Override
	public void close() {
		this.beanstalk.close();
	}

}