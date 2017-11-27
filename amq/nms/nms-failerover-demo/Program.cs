using System;
using System.Threading;
using Apache.NMS;
using Apache.NMS.Util;

namespace nms_client_demo
{
    class Program
    {
        static void Main(string[] args)
        {      
            Uri connecturi1 = new Uri("tcp://localhost:61616");         
            Uri connecturi2 = new Uri("tcp://localhost:61617");         
            Uri failoveruri = new Uri("activemq:failover:tcp://localhost:61616,tcp://localhost:61617");         

            Thread producerThread = new Thread(new ThreadStart(RunProducer));
            producerThread.Start();
            Thread consumerThread = new Thread(new ThreadStart(RunConsumer));
            consumerThread.Start();

                        Thread consumerThread1 = new Thread(new ThreadStart(RunConsumer));
            consumerThread1.Start();

                        Thread consumerThread2 = new Thread(new ThreadStart(RunConsumer));
            consumerThread2.Start();

                        Thread consumerThread3 = new Thread(new ThreadStart(RunConsumer));
            consumerThread3.Start();


            void RunProducer()
            {
                IConnectionFactory factory = new NMSConnectionFactory(failoveruri);
        
                using(IConnection connection = factory.CreateConnection())
                {
                    connection.ClientId = "sample-producer";            
                    using(ISession session = connection.CreateSession())
                    {                

                        // One topic for store 123 on address "orders"
                        ITopic ordersTopic = SessionUtil.GetTopic(session, "topic://orders");        

                        using(IMessageProducer olineOrderProducer = session.CreateProducer())
                        {
                            // Start the connection so that messages will be processed.
                            connection.Start();

                            // Set message reliability
                            olineOrderProducer.DeliveryMode = MsgDeliveryMode.Persistent;
            
                            while (true)
                            {
                                // Send a message to each store topic
                                olineOrderProducer.Send(ordersTopic, session.CreateTextMessage("order placed!"));
                                Thread.Sleep(1000);
                            }
                        }
                    }   
                }        
            }    
        
            void RunConsumer()
            {
                IConnectionFactory factory = new NMSConnectionFactory(failoveruri);
        
                using(IConnection connection = factory.CreateConnection())
                {
                    connection.ClientId = Guid.NewGuid().ToString();            
                    using(ISession session = connection.CreateSession())
                    {                

                        // One topic for store 123 on address "orders"
                        ITopic ordersTopic = SessionUtil.GetTopic(session, "topic://orders");        

                        using(IMessageConsumer someConsumer = session.CreateDurableConsumer(ordersTopic, "orders-consumer", null, false))
                        {
                            // Start the connection so that messages will be processed.
                            connection.Start();
            
                            while (true)
                            {
                                ITextMessage message = someConsumer.ReceiveNoWait() as ITextMessage;
                                if (message != null)
                                {
                                    Console.WriteLine("recieved: " + message.Text);
                                }
                            }
                        }
                    }   
                }        
            }    
        }
    }                
}
    
