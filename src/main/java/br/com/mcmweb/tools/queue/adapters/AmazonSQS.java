package br.com.mcmweb.tools.queue.adapters;

import java.util.ArrayList;
import java.util.List;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class AmazonSQS extends GenericQueue {

	private AmazonSQSAsyncClient sqs;

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
	public String put(Object object) {
		SendMessageRequest messageRequest = new SendMessageRequest(this.host, this.serializeMessageBody(object));
		SendMessageResult messageResult = this.sqs.sendMessage(messageRequest);
		return messageResult.getMessageId();
	}

//	@Override
//	public List<String> put(List<Object> object) {
//		// TODO Auto-generated method stub
//		return null;
//	}

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
					// TODO log warning
				}
			}
			MessageResponse response = this.unserializeMessageBody(messageSQS.getMessageId(), messageSQS.getReceiptHandle(), receivedCount, messageSQS.getBody());
			return response;
		}

		return null;
	}

	@Override
	public Boolean delete(MessageResponse message) {
		DeleteMessageRequest messageRequest = new DeleteMessageRequest(this.host, message.getHandle());
		try {
			this.sqs.deleteMessage(messageRequest);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

//	@Override
//	public List<Boolean> delete(List<MessageResponse> responseList) {
//		List<DeleteMessageBatchRequestEntry> entries = new ArrayList<DeleteMessageBatchRequestEntry>();
//		for (MessageResponse response : responseList) {
//			DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry();
//			entry.setId(response.getId());
//			entry.setReceiptHandle(response.getHandle());
//			entries.add(entry);
//		}
//		DeleteMessageBatchRequest messageRequest = new DeleteMessageBatchRequest(this.host, entries);
//		DeleteMessageBatchResult result = this.sqs.deleteMessageBatch(messageRequest);
//
//		List<Boolean> status = new ArrayList<Boolean>();
//		List<DeleteMessageBatchResultEntry> successfullList = result.getSuccessful();
//		List<BatchResultErrorEntry> failedList = result.getFailed();
//		for (MessageResponse response : responseList) {
//			boolean found = false;
//			for (DeleteMessageBatchResultEntry successfull : successfullList) {
//				if (successfull.getId().equals(response.getId())) {
//					found = true;
//					status.add(true);
//					continue;
//				}
//			}
//			if (found) {
//				continue;
//			}
//			for (BatchResultErrorEntry failed : failedList) {
//				if (failed.getId().equals(response.getId())) {
//					found = true;
//					status.add(false);
//					continue;
//				}
//			}
//			if (found) {
//				continue;
//			}
//		}
//
//		return status;
//	}

	@Override
	public void close() {
		this.sqs.shutdown();
	}

}
