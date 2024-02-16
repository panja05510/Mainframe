package com.citizens.mainframe.service;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import com.citizens.mainframe.model.AccountDetails;
import com.citizens.mainframe.model.SavingsAccount;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.ibm.jakarta.jms.JMSMessage;

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;

@Service
public class QueueListnerAndResponse {
	@Autowired
	AccountDetails accountDetails;
	
	ObjectMapper objectMapper;
	

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private Convertions convertions;

	private final JmsTemplate jmsTemplate;
	private final ThreadPoolTaskExecutor taskExecutor;

	private long messageReceivedTime;
	private long replySentTime;
	private long timeTakenToProcessAndSendReply;

	public QueueListnerAndResponse(JmsTemplate jmsTemplate, ThreadPoolTaskExecutor taskExecutor) {
		this.jmsTemplate = jmsTemplate;
		this.taskExecutor = taskExecutor;
//		this.accountDetails=accountDetails;
	}

	@JmsListener(destination = "DEV.QUEUE.1")
	public void receiveMessage(JMSMessage receivedMessage) {
		try {
		

		//System.out.println("Request Received to Mainframe: " );
		messageReceivedTime = System.currentTimeMillis();
//		logger.info("Mainframe:Received Message : {}", messageReceivedTime);

		if (receivedMessage != null)
//			logger.info("MainFrame message received corrID-> : {}", receivedMessage.getJMSCorrelationID());
//		System.out.println("Message received to Mainframe: "+ebcdicToObject(receivedMessage));
		System.out.println("Message received to mainframe");
		// System.out.println("MainFrame message received corrID->" +
		// receivedMessage.getJMSCorrelationID());
		

		replyAsync(receivedMessage, messageReceivedTime);
		}
		catch(Exception e){
			System.err.println(e.getMessage());
			
		}
	}
		

	public void replyAsync(JMSMessage receivedMessage, long messageReceisvedTime) {
		taskExecutor.execute(() -> {
			try {
				System.out.println("processing....");
				Thread.sleep(10000);

				// System.out.println("Reply sent: " + replyMessage);

				//logger.info("Mainframe:Thread ID : {} and time taken {}", Thread.currentThread().getId(),
					//	timeTakenToProcessAndSendReply);
				// System.out.println(Thread.currentThread().getId());

				jmsTemplate.send("DEV.QUEUE.2", session -> {
					BytesMessage message = session.createBytesMessage();
					try {
						message.writeBytes(processEbcdic());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					message.setJMSCorrelationID(receivedMessage.getJMSCorrelationID());
					return message;
				});
				System.out.println("Response sent to Queue.2....");

//				replySentTime = System.currentTimeMillis();
//				logger.info("Mainframe:Reply sent : {}", replySentTime);
//				timeTakenToProcessAndSendReply = replySentTime - messageReceivedTime;

			} catch (Exception e) {
				e.printStackTrace();
			}

		});
	}

	public byte[] processEbcdic() throws IOException {

//		
		accountDetails.setName("Jagadeesh");
		accountDetails.setBalance(12345);
		accountDetails.setIfsc("SBIN0020538");
		accountDetails.setAccountType("savings");

		byte[] ebcdicResultData = convertions.jsonToEbcdic(accountDetails);
//		System.out.println(ebcdicResultData);

		return ebcdicResultData;
	}
	private Object ebcdicToObject(JMSMessage message) throws JMSException, IOException{
		BytesMessage receivedMessage = (BytesMessage)message;

		byte[] bytes = new byte[(int) receivedMessage.getBodyLength()];
		receivedMessage.readBytes(bytes);
//		System.out.println(bytes);
//		for (byte b : bytes)
//			System.out.println(b);
		String str = convertions.ebcdicToJson(bytes);
		SavingsAccount savingsAccount = objectMapper.readValue(str, SavingsAccount.class);
		return savingsAccount;
		
		
		
		
	}
}
