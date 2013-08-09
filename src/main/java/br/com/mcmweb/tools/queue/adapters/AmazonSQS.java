package br.com.mcmweb.tools.queue.adapters;

import java.util.List;
import java.util.logging.Logger;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class AmazonSQS extends GenericQueue {

	private AmazonSQSAsyncClient sqs;
	private static final Logger logger = Logger.getLogger(AmazonSQS.class.getName());

	/**
	 * Initialize queue connections
	 * 
	 * @param host
	 *            should be the queue URL (including http://...)
	 * @param login
	 *            should be the access key id
	 * @param password
	 *            should be the secret access key
	 * @param name
	 *            queue name
	 * @throws Exception
	 */
	public AmazonSQS(String host, String login, String password, String name) throws Exception {
		super(host, login, password, name);
		this.connect();
	}

	@Override
	public void connect() throws Exception {
		AWSCredentials credentials = new BasicAWSCredentials(this.login, this.password);
		this.sqs = new AmazonSQSAsyncClient(credentials);
	}

	@Override
	public boolean put(Object object) {
		SendMessageRequest messageRequest = new SendMessageRequest(this.host, this.serializeMessageBody(object));
		try {
			SendMessageResult messageResult = this.sqs.sendMessage(messageRequest);
			logger.finest("Added message do Amazon SQS, id " + messageResult.getMessageId());
			return true;
		} catch (Exception e) {
			logger.severe("Error adding message to Amazon SQS");
			logger.severe(e.getMessage());			
			return false;
		}
	}

	@Override
	public MessageResponse getNext() {
		ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(this.host);
		receiveRequest.setMaxNumberOfMessages(1); // FIXME config or parameter
		ReceiveMessageResult receiveResult = this.sqs.receiveMessage(receiveRequest);

		List<Message> messageSQSList = receiveResult.getMessages();
		if (messageSQSList.size() > 0) {
			Message messageSQS = messageSQSList.get(0);
			Integer receivedCount = null;
			if (messageSQS.getAttributes() != null) {
				try {
					receivedCount = Integer.parseInt(messageSQS.getAttributes().get("ApproximateReceiveCount"));
				} catch (Exception e) {
					logger.warning("Unable to determine received count: " + e.getMessage());
				}
			}
			MessageResponse response = this.unserializeMessageBody(messageSQS.getMessageId(), messageSQS.getReceiptHandle(), receivedCount, messageSQS.getBody());
			return response;
		}

		return null;
	}

	@Override
	public boolean delete(MessageResponse message) {
		DeleteMessageRequest messageRequest = new DeleteMessageRequest(this.host, message.getHandle());
		try {
			this.sqs.deleteMessage(messageRequest);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public boolean release(MessageResponse response, Integer delaySeconds) {
		if (delaySeconds == null) {
			delaySeconds = 0;
		}
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(this.host, response.getHandle(), delaySeconds);
		try {
			this.sqs.changeMessageVisibility(request);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public boolean touch(MessageResponse response) {
		// TODO replace 60 seconds to something better
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(this.host, response.getHandle(), 60);
		try {
			this.sqs.changeMessageVisibility(request);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public void close() {
		this.sqs.shutdown();
	}

}