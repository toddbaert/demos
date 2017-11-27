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

            int numProducers = args[0] != null ? Int32.Parse(args[1]) : 1;
            int numConsumers = args[1] != null ? Int32.Parse(args[1]) : 1;           

            Uri failoveruri = new Uri("activemq:failover:tcp://localhost:61616,tcp://localhost:61617");     
            
            for (int i=0; i<numProducers; i++)
            {
                var id = i;
                Thread producerThread = new Thread(() => RunProducer(id));
                producerThread.Start();
                Thread.Sleep(1000);
            }

            for (int i=0; i<numConsumers; i++)
            {
                var id = i;
                Thread consumerThread = new Thread(() => RunConsumer(id));
                consumerThread.Start();
                Thread.Sleep(1000);
            }

            void RunProducer(int id)
            {
                Console.WriteLine("Producer " + id + " is connecting...");
                IConnectionFactory factory = new NMSConnectionFactory(failoveruri);
        
                using(IConnection connection = factory.CreateConnection())
                {
                    connection.ClientId = "producer" + id;            
                    using(ISession session = connection.CreateSession())
                    {                

                        ITopic ordersTopic = SessionUtil.GetTopic(session, "topic://orders");        

                        using(IMessageProducer olineOrderProducer = session.CreateProducer())
                        {
                            connection.Start();

                            olineOrderProducer.DeliveryMode = MsgDeliveryMode.Persistent;
            
                            while (true)
                            {
                                olineOrderProducer.Send(ordersTopic, session.CreateTextMessage("order placed!"));
                                Thread.Sleep(1000);
                            }
                        }
                    }   
                }        
            }    
        
            void RunConsumer(int id)
            {
                Console.WriteLine("Consumer " + id + " is connecting...");

                IConnectionFactory factory = new NMSConnectionFactory(failoveruri);
        
                using(IConnection connection = factory.CreateConnection())
                {
                    connection.ClientId = "consumer" + id;            
                    using(ISession session = connection.CreateSession())
                    {                
                        ITopic ordersTopic = SessionUtil.GetTopic(session, "topic://orders");        

                        using(IMessageConsumer someConsumer = session.CreateDurableConsumer(ordersTopic, "orders-consumer", null, false))
                        {
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
    
