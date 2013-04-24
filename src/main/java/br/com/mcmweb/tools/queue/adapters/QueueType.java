package br.com.mcmweb.tools.queue.adapters;

public enum QueueType {

	AMAZON_SQS(AmazonSQS.class), //
	BEANSTALKD(Beanstalk.class);

	private Class<? extends GenericQueue> clazz;

	QueueType(Class<? extends GenericQueue> clazz) {
		this.clazz = clazz;
	}

	public Class<? extends GenericQueue> clazz() {
		return clazz;
	}

}
