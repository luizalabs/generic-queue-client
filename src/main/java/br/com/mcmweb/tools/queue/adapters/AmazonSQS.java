package br.com.mcmweb.tools.queue.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import br.com.mcmweb.tools.queue.messages.MessageResponse;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
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
	}

	@Override
	protected void connect() throws Exception {
		AWSCredentials credentials = new BasicAWSCredentials(this.login, this.password);
		this.sqs = new AmazonSQSAsyncClient(credentials);
	}

	@Override
	protected boolean reconnect() {
		// ignored by sqs
		return true;
	}

	@Override
	public boolean put(Object object) {
		SendMessageRequest messageRequest = new SendMessageRequest(this.host, this.serializeMessageBody(object));
		int retries = 0;
		do {
			try {
				SendMessageResult messageResult = this.sqs.sendMessage(messageRequest);
				logger.finest("Added message to Amazon SQS, id " + messageResult.getMessageId());
				return true;
			} catch (AmazonServiceException e) {
				logger.severe("Error adding message to Amazon SQS: " + e);
				if (e.getErrorType() == ErrorType.Client) {
					break;
				}
				reconnectSleepTimer();
				retries++;
				logger.info("Retrying SQS put Command! Reason: " + e);
			} catch (Exception e) {
				logger.severe("Error adding message to Amazon SQS: " + e);
				break;
			}
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	@Override
	public MessageResponse getNext() {
		try {
			ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(this.host);

			ArrayList<String> attributeNames = new ArrayList<String>();
			attributeNames.add("ApproximateReceiveCount");
			attributeNames.add("SentTimestamp");
			receiveRequest.setAttributeNames(attributeNames);

			receiveRequest.setMaxNumberOfMessages(1); // FIXME config or parameter
			ReceiveMessageResult receiveResult = this.sqs.receiveMessage(receiveRequest);

			List<Message> messageSQSList = receiveResult.getMessages();
			if (messageSQSList.size() > 0) {
				Message messageSQS = messageSQSList.get(0);
				MessageResponse response = this.unserializeMessageBody(messageSQS.getMessageId(), messageSQS.getReceiptHandle(), messageSQS.getBody());
				
				if (response.getAge() == 0) {
					try {
						Long creation = Long.parseLong(messageSQS.getAttributes().get("SentTimestamp"));
	
						DateTimeZone.setDefault(DateTimeZone.UTC);
						Long now = (long) DateTime.now().getMillis();
						
						response.setAge((long) (now - creation) / 1000);
					} catch (Exception e) {
						logger.info("No timestamp sent by AmazonSQS... setting it to zero.");
					}
				}

				String receiveCount = messageSQS.getAttributes().get("ApproximateReceiveCount");
				Boolean isRedeliver = false;
				if (receiveCount != null)
					isRedeliver = (Integer.parseInt(receiveCount) > 1);
				else
					logger.info("No receive count received, metadata not available.");
				response.setIsRedeliver(isRedeliver);

				return response;
			}
		} catch (Exception e) {
			logger.info("Unknown error! Reason: " + e);
		}

		return null;
	}

	@Override
	public boolean delete(MessageResponse message) {
		DeleteMessageRequest messageRequest = new DeleteMessageRequest(this.host, message.getHandle());
		int retries = 0;
		do {
			try {
				this.sqs.deleteMessage(messageRequest);
				return true;
			} catch (AmazonServiceException e) {
				logger.severe("Error deleting SQS message: " + e);
				if (e.getErrorType() == ErrorType.Client) {
					break;
				}
				reconnectSleepTimer();
				retries++;
				logger.info("Retrying SQS Delete Command! Reason: " + e);
			} catch (Exception e) {
				logger.severe("Error deleting SQS message: " + e);
				break;
			}
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	@Override
	public boolean release(MessageResponse response, Integer delaySeconds) {
		if (delaySeconds == null) {
			delaySeconds = 0;
		}
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(this.host, response.getHandle(), delaySeconds);
		int retries = 0;
		do {
			try {
				this.sqs.changeMessageVisibility(request);
				return true;
			} catch (AmazonServiceException e) {
				logger.severe("Error releasing SQS message: " + e);
				if (e.getErrorType() == ErrorType.Client) {
					break;
				}
				reconnectSleepTimer();
				retries++;
				logger.info("Retrying SQS Release Command! Reason: " + e);
			} catch (Exception e) {
				logger.severe("Error releasing SQS message: " + e);
				break;
			}
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	@Override
	public boolean touch(MessageResponse response) {
		// TODO replace 60 seconds to something better
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(this.host, response.getHandle(), 60);
		int retries = 0;
		do {
			try {
				this.sqs.changeMessageVisibility(request);
				return true;
			} catch (AmazonServiceException e) {
				logger.severe("Error touching SQS message: " + e);
				if (e.getErrorType() == ErrorType.Client) {
					break;
				}
				reconnectSleepTimer();
				retries++;
				logger.info("Retrying SQS Touch Command! Reason: " + e);
			} catch (Exception e) {
				logger.severe("Error touching SQS message: " + e);
				break;
			}
		} while (retries < CONNECTION_RETRIES);
		return false;
	}

	@Override
	@PreDestroy
	public void close() {
		this.sqs.shutdown();
	}

}