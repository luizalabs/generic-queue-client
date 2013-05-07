package br.com.mcmweb.tools.queue.adapters;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import br.com.mcmweb.tools.queue.messages.MessageRequest;
import br.com.mcmweb.tools.queue.messages.MessageResponse;

public abstract class GenericQueue {

	protected String host;
	protected String login;
	protected String password;
	protected String queueName;
	protected ObjectMapper mapper;

	public GenericQueue(String host, String login, String password, String queueName) throws Exception {
		this.host = host;
		this.login = login;
		this.password = password;
		this.queueName = queueName;
		this.mapper = new ObjectMapper();
		this.mapper.setSerializationInclusion(Inclusion.NON_EMPTY);
	}

	/**
	 * Connect to queue
	 * 
	 * @throws Exception
	 */
	public abstract void connect() throws Exception;

	/**
	 * Add message to queue
	 * 
	 * @param object
	 * @return
	 */
	public abstract String put(Object object);

	/**
	 * Remove message from queue
	 * 
	 * @param response
	 * @return
	 */
	public abstract Boolean delete(MessageResponse response);

	/**
	 * Release message back to queue, in delaySeconds
	 * 
	 * @param response
	 * @param delaySeconds
	 * @return
	 */
	public abstract Boolean release(MessageResponse response, Integer delaySeconds);

	/**
	 * Touch queue message to avoid timeouts
	 * 
	 * @param response
	 * @return
	 */
	public abstract Boolean touch(MessageResponse response);

	/**
	 * Retrieve next queued message
	 * 
	 * @return
	 */
	public abstract MessageResponse getNext();

	/**
	 * Close queue connection
	 */
	public abstract void close();

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	/**
	 * Create MessageRequest object to wrap your object and serialize everything
	 * 
	 * @param object
	 *            to be queued
	 * @return string to be queued
	 */
	protected String serializeMessageBody(Object object) {
		String fullMessageBody = null;
		try {
			String body = this.mapper.writeValueAsString(object);
			MessageRequest messageRequest = new MessageRequest();
			messageRequest.setType(object.getClass().getCanonicalName());
			messageRequest.setBody(body);
			fullMessageBody = this.mapper.writeValueAsString(messageRequest);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fullMessageBody;
	}

	protected MessageResponse unserializeMessageBody(String id, String handle, Integer receivedCount, String body) {
		MessageResponse messageResponse = new MessageResponse();
		messageResponse.setId(id);

		if (handle != null) {
			messageResponse.setHandle(handle);
		} else {
			messageResponse.setHandle(id);
		}

		if (receivedCount == null) {
			receivedCount = 0;
		}
		messageResponse.setReceivedCount(receivedCount);

		if (body != null && !"".equals(body)) {
			try {
				MessageRequest messageRequest = mapper.readValue(body, MessageRequest.class);
				messageResponse.setType(messageRequest.getType());
				messageResponse.setObject(mapper.readValue(messageRequest.getBody(), Class.forName(messageRequest.getType())));
				return messageResponse;
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}

}
