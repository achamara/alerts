package com.caffinc.alerts;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Alert System based on Amazon Simple Queue Service
 * @author Sriram
 */
public class Alerts
{
    private static final Logger LOG = LoggerFactory.getLogger( Alerts.class );
    private static final String DEFAULT_CREDENTIALS_LOCATION = "/sqscredentials2.properties";
    private String queueName;
    private String queueUrl;
    private AmazonSQS queueService;
    private AtomicBoolean isReceiving;


    /**
     * Callback handler for Alerts
     * @author Sriram
     */
    public static abstract class AlertCallback
    {
        /**
         * Called when the Alerts system receives a message. Handled messages are removed from the Queue.
         * @param queueName Name of the queue that sent the message
         * @param message Message received
         * @return true if message is handled, false otherwise
         */
        abstract public boolean handle( String queueName, Message message );
    }


    /**
     * Connects to the SQS service and configures to the queueName
     * @param queueName Queue name to send to/receive from
     */
    public Alerts( String queueName )
    {
        this( queueName, DEFAULT_CREDENTIALS_LOCATION );
    }


    /**
     * Connects to the SQS service and configures to the queueName
     * @param queueName Queue name to send to/receive from
     * @param credentialsFile File name to read the credentials from
     */
    public Alerts( String queueName, String credentialsFile )
    {
        this.queueName = queueName;
        this.isReceiving = new AtomicBoolean( false );
        this.queueService = this.connect( credentialsFile );
        this.queueUrl = this.findQueueUrl( this.queueName );
    }


    /**
     * Reads the credentials from the file and connects to Amazon SQS service.
     * @param credentialsFile File to read the credentials from
     * @return AmazonSQS client
     */
    private AmazonSQS connect( String credentialsFile )
    {
        LOG.info( "Reading credentials from properties file..." );
        AWSCredentials credentials;
        try {
            credentials = new PropertiesCredentials( Alerts.class.getResourceAsStream(
                credentialsFile ) );
        } catch ( IOException e ) {
            throw new AmazonClientException(
                "Cannot load the credentials from the credential profiles file: " + credentialsFile,
                e );
        }

        LOG.info( "Connecting to Amazon SQS Service..." );
        return new AmazonSQSClient( credentials );
    }


    /**
     * Attempts to find the Queue URL for the given Queue name
     * @param queueName Queue name to search for
     * @return Queue URL
     */
    private String findQueueUrl( String queueName )
    {
        LOG.info( "Searching QueueURL for Queue {}", queueName );
        ListQueuesRequest listQueuesRequest = new ListQueuesRequest();
        listQueuesRequest.setQueueNamePrefix( queueName );
        for ( String queueUrl : queueService.listQueues( listQueuesRequest ).getQueueUrls() ) {
            if ( queueUrl.endsWith( queueName ) ) {
                LOG.info( "Found {}", queueUrl );
                return queueUrl;
            }
        }
        LOG.info( "Queue {} not found", queueName );
        return null;
    }


    /**
     * Creates the queue
     * @return Queue URL of the created queue
     */
    public String createQueue()
    {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest( queueName );
        this.queueUrl = queueService.createQueue( createQueueRequest ).getQueueUrl();
        return queueUrl;
    }


    /**
     * Returns the list of queues for this account
     * @return List of Queues
     */
    public List<String> listQueues()
    {
        return this.queueService.listQueues().getQueueUrls();
    }


    /**
     * Deletes a message from the Queue
     * @param message Message to delete
     */
    public void deleteMessage( Message message )
    {
        LOG.debug( "Deleting message {}", message.getMessageId() );
        String messageReceiptHandle = message.getReceiptHandle();
        queueService.deleteMessage( new DeleteMessageRequest( queueUrl, messageReceiptHandle ) );
    }


    /**
     * Sends a message
     * @param message Message to send
     */
    public void sendMessage( String message )
    {
        queueService.sendMessage( new SendMessageRequest( queueUrl, message ) );
    }


    /**
     * Starts the QueueReceiver thread to poll the SQS service for messages and calls the AlertCallback when a message
     * is received
     * @param callback AlertCallback to handle messages
     */
    public void receiveMessages( final AlertCallback callback )
    {
        isReceiving.set( true );
        new Thread( "QueueReceiver_" + queueName )
        {
            @Override
            public void run()
            {
                while ( isReceiving.get() ) {
                    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest( queueUrl );
                    List<Message> messages = queueService.receiveMessage( receiveMessageRequest ).getMessages();
                    for ( Message message : messages ) {
                        if ( callback.handle( queueName, message ) ) {
                            deleteMessage( message );
                        }
                    }

                }
            }
        }.start();
    }


    /**
     * Stops receiving messages, must be called to stop the QueueReceiver thread
     */
    public void stop()
    {
        isReceiving.set( false );
    }


    /**
     * Deletes the queue
     */
    public void deleteQueue()
    {
        LOG.warn( "Deleting queue {}", queueUrl );
        queueService.deleteQueue( new DeleteQueueRequest( queueUrl ) );
    }


    /**
     * Deletes all the queues associated with the account
     * (This method is for debug purposes only, delete in production)
     */
    public void deleteAllQueues()
    {
        for ( String queueUrl : queueService.listQueues().getQueueUrls() ) {
            this.queueUrl = queueUrl;
            deleteQueue();
        }
    }

}