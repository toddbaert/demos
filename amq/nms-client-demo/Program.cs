using System;
using Apache.NMS;
using Apache.NMS.Util;

namespace nms_client_demo
{
    class Program
    {
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
                    ITopic store123Topic = SessionUtil.GetTopic(session, "topic://orders.s123");        
                    
                    // Another for store 456    
                    ITopic store456Topic = SessionUtil.GetTopic(session, "topic://orders.s456");    

                    // A third listening andthing under "orders"            
                    ITopic allOrdersTopic = SessionUtil.GetTopic(session, "topic://orders.>");                      

                    // Instantiate Consumers
                    using(IMessageConsumer store123Consumer = session.CreateDurableConsumer(store123Topic, "store123", null, false))
                    using(IMessageConsumer store456Consumer = session.CreateDurableConsumer(store456Topic, "store456", null, false))
                    using(IMessageConsumer biConsumer = session.CreateDurableConsumer(allOrdersTopic, "bi", null, false))
                    using(IMessageConsumer auditingConsumer = session.CreateDurableConsumer(allOrdersTopic, "auditing", null, false))
                    using(IMessageProducer olineOrderProducer = session.CreateProducer())
                    {
                        // Start the connection so that messages will be processed.
                        connection.Start();

                        // Set message reliability
                        olineOrderProducer.DeliveryMode = MsgDeliveryMode.Persistent;
        
                        // Send a message to each store topic
                        olineOrderProducer.Send(store123Topic, session.CreateTextMessage("order placed for store 123!"));
                        olineOrderProducer.Send(store456Topic, session.CreateTextMessage("order placed for store 456!"));        
                        
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
    
