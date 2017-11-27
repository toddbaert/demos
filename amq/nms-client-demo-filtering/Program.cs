using System;
using Apache.NMS;
using Apache.NMS.Util;

namespace nms_client_demo_filtering
{
    class Program
    {
        private static readonly String STORE = "store";

        static void Main(string[] args)
        {
        
        Uri connecturi = new Uri("tcp://localhost:61616");         
        Console.WriteLine("Connecting to " + connecturi);
 
        // NOTE: ensure the nmsprovider-activemq.config file exists in the executable folder.
        IConnectionFactory factory = new NMSConnectionFactory(connecturi);
 
            using(IConnection connection = factory.CreateConnection())
            {
                connection.ClientId = "sample-client";            
                using(ISession session = connection.CreateSession())
                {                

                    // One topic for store 123 on address "orders"
                    ITopic ordersTopic = SessionUtil.GetTopic(session, "topic://orders");                            

                    // Instantiate consumers
                    using(IMessageConsumer store123Consumer = session.CreateDurableConsumer(ordersTopic, "store123", "store = 123", false))
                    using(IMessageConsumer store456Consumer = session.CreateDurableConsumer(ordersTopic, "store456", "store = 456", false))
                    using(IMessageConsumer biConsumer = session.CreateDurableConsumer(ordersTopic, "bi", null, false))
                    using(IMessageConsumer auditingConsumer = session.CreateDurableConsumer(ordersTopic, "auditing", null, false))
                    using(IMessageProducer olineOrderProducer = session.CreateProducer())
                    {
                        // Start the connection so that messages will be processed.
                        connection.Start();

                        // Set message reliability
                        olineOrderProducer.DeliveryMode = MsgDeliveryMode.Persistent;
        
                        // Create order with property for store 123
                        ITextMessage orderFor123 = session.CreateTextMessage("order placed for store 123!");
                        orderFor123.Properties[STORE] = 123; 
                        ITextMessage orderFor456 = session.CreateTextMessage("order placed for store 456!");
                        orderFor456.Properties[STORE] = 456; 

                        // Send a message to each store topic
                        olineOrderProducer.Send(ordersTopic, orderFor123);
                        olineOrderProducer.Send(ordersTopic, orderFor456);        
                        
                        // Blocking consumer, will block for up to one second
                        ITextMessage message = store123Consumer.Receive(TimeSpan.FromSeconds(1)) as ITextMessage;
                        Console.WriteLine("Store 123 Consumer received message with text: " + message.Text);

                        // Blocking consumer, but will not wait if no messages are immediately available
                        message = store456Consumer.ReceiveNoWait() as ITextMessage;
                        Console.WriteLine("Store 456 Consumer received message with text: " + message.Text);
                        
                        // Async consumers, functions with callback
                        biConsumer.Listener += (IMessage asyncMessage) => {
                            Console.WriteLine("BI Consumer received message with text: " + (asyncMessage as ITextMessage).Text);            
                        };
                        auditingConsumer.Listener += (IMessage asyncMessage) => {
                            Console.WriteLine("Auditing Consumer received message with text: " + (asyncMessage as ITextMessage).Text);            
                        };
                    }
                }   
            }        
        }
    }
}
    
