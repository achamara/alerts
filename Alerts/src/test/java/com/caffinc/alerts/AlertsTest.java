package com.caffinc.alerts;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;


/**
 * Tests the Alert system
 * @author Sriram
 */
public class AlertsTest
{
    private static final Logger LOG = LoggerFactory.getLogger( AlertsTest.class );
    private static final Random random = new Random();
    private static final String testQueueName = "test_" + random.nextInt();
    private static String testQueueUrl = null;
    private static final AmazonSQS testQueueService;

    static {
        try {
            testQueueService = new AmazonSQSClient(
                new PropertiesCredentials( Alerts.class.getResourceAsStream( "/sqscredentials2.properties" ) ) );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read credentials to create the AmazonSQS service for testing" );
        }
    }

    /**
     * Creates a dummy queue for testing
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        testQueueUrl = testQueueService.createQueue( new CreateQueueRequest( testQueueName ) ).getQueueUrl();
        testQueueService.sendMessage( new SendMessageRequest( testQueueUrl, "Test message" ) );
        Thread.sleep( 1000 );
    }


    /**
     * Tests if the Alerts service connects to AmazonSQS
     */
    @Test
    public void testConnect()
    {
        new Alerts( testQueueName ).listQueues();
    }


    /**
     * Tests if the Alerts service is able to list queues
     */
    @Test
    public void testListQueues()
    {
        testQueueService.receiveMessage( new ReceiveMessageRequest( testQueueUrl ) );
        assertTrue( "Alerts service must list all queues", new Alerts( testQueueName ).listQueues().contains
            ( testQueueUrl ) );
    }


    /**
     * Tests if the Alerts service can create a Queue
     */
    @Test
    public void testCreateQueue() throws InterruptedException
    {
        Alerts alerts = new Alerts( "test_" + random.nextInt() );
        String queueUrl = alerts.createQueue();
        String messageBody = "Test message";
        testQueueService.sendMessage( new SendMessageRequest( queueUrl, messageBody ) );
        String receivedMessageBody = testQueueService.receiveMessage( new ReceiveMessageRequest( queueUrl ) )
            .getMessages().get( 0 ).getBody();
        testQueueService.deleteQueue( new DeleteQueueRequest( queueUrl ) );
        assertTrue( "Queue must be created and ready to send messages", messageBody.equals( receivedMessageBody ) );
    }


    /**
     * Tests if the Alerts service can send a message
     */
    @Test public void testSend()
    {
        Alerts alerts = new Alerts( "test_" + random.nextInt() );
        String queueUrl = alerts.createQueue();
        boolean messageFound = false;
        String messageBody = "This is a send test message for AmazonSQS";
        alerts.sendMessage( messageBody );
        for ( Message message : testQueueService.receiveMessage( new ReceiveMessageRequest( queueUrl ) )
            .getMessages() ) {
            if ( messageBody.equals( message.getBody() ) ) {
                printMessage( message );
                messageFound = true;
                break;
            }
        }
        assertTrue( "Message sent should be found in the queue", messageFound );
    }


    /**
     * Tests if the Alerts service can receive a message
     */
    @Test public void testReceive() throws InterruptedException
    {
        final AtomicBoolean messageReceived = new AtomicBoolean( false );
        String queueName = "test_" + random.nextInt();
        String queueUrl = testQueueService.createQueue( new CreateQueueRequest( queueName ) ).getQueueUrl();
        final String messageBody = "This is a receive test message for AmazonSQS";
        testQueueService.sendMessage( new SendMessageRequest( queueUrl, messageBody ) );
        Alerts alerts = new Alerts( queueName );
        alerts.receiveMessages( new Alerts.AlertCallback()
        {
            @Override public boolean handle( String queueName, Message message )
            {
                if ( messageBody.equals( message.getBody() ) ) {
                    printMessage( message );
                    messageReceived.set( true );
                    return true;
                }
                return false;
            }
        } );
        long startTime = System.currentTimeMillis();
        while ( !messageReceived.get() && System.currentTimeMillis() - startTime < 10000 ) {
            Thread.sleep( 10 );
        }
        assertTrue( "Message sent on the queue should be received", messageReceived.get() );
    }


    /**
     * Tests if the Alerts service can delete a queue. Exception is thrown if send is attempted on a deleted queue.
     * There is no way to test if it actually deleted as Amazon may take up to 60 seconds to actually delete the queue.
     */
    @Test (expected = QueueDoesNotExistException.class) public void testDeleteQueue() throws Exception
    {
        String queueName = "test_" + random.nextInt();
        String queueUrl = testQueueService.createQueue( new CreateQueueRequest( queueName ) ).getQueueUrl();
        Alerts alerts = new Alerts( queueName );
        alerts.deleteQueue();
        testQueueService.sendMessage( new SendMessageRequest( queueUrl, "This is a test" ) );
    }


    /**
     * Deletes the testQueue
     */
    @AfterClass
    public static void tearDown()
    {
        testQueueService.deleteQueue( new DeleteQueueRequest( testQueueUrl ) );
    }


    /**
     * Tests the SQS Service
     * (This method is for debug purposes only, delete in production)
     * @throws Exception
     */
    @Ignore
    @Test
    public void testAll() throws Exception
    {
        final Alerts alerts = new Alerts( "test" + ( new Random() ).nextInt() );
        alerts.createQueue();
        alerts.sendMessage( "{\"customerKey\":\"amg112\", \"message\": \"New feed is ready\", \"sender\": "
            + "\"Inferlytics Analyser Feed\"}" );
        alerts.receiveMessages( new Alerts.AlertCallback()
        {
            @Override public boolean handle( String queueName, Message message )
            {
                LOG.info( "Received message on {}: ", queueName );
                printMessage( message );
                return true;
            }
        } );
        Thread.sleep( 10000 );
        alerts.stop();
        alerts.deleteQueue();
    }


    /**
     * Deletes all queues
     */
    @Ignore
    @Test
    public void deleteAllQueues()
    {
        new Alerts( testQueueName ).deleteAllQueues();
    }


    /**
     * Prints the message on the screen
     * @param message Message to be printed
     */
    private static void printMessage( Message message )
    {
        LOG.info( "  Message" );
        LOG.info( "    MessageId:     " + message.getMessageId() );
        LOG.info( "    ReceiptHandle: " + message.getReceiptHandle() );
        LOG.info( "    MD5OfBody:     " + message.getMD5OfBody() );
        LOG.info( "    Body:          " + message.getBody() );
        for ( Map.Entry<String, String> entry : message.getAttributes().entrySet() ) {
            LOG.info( "  Attribute" );
            LOG.info( "    Name:  " + entry.getKey() );
            LOG.info( "    Value: " + entry.getValue() );
        }
    }
}
