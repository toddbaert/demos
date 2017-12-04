using System;
using System.Collections.Generic;
using System.Threading;
using System.Transactions;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;

namespace amqp_client_demo
{
    class Program
    {
        static void Main(string[] args)
        {
<<<<<<< HEAD
            string url = "amqp://localhost:5672";
            String ADDRESS = "orders";
            Address peerAddr = new Address(url);                 
                       
            // start 2 consumer threads, each with own connection/session, mimicking dinstinct processes.
            // characters before "|" make up the subscription name on the broker, characters after are ignored, but allow client to identify the subs
            Console.WriteLine("Starting consumer threads...");
            new Thread(() => RunConsumer("BI|1", ADDRESS)).Start();
            Thread.Sleep(TimeSpan.FromSeconds(1));
            new Thread(() => RunConsumer("BI|2", ADDRESS)).Start();
        
            void RunConsumer(String subscriptionName, String target)
            {
                Connection connection = new Connection(peerAddr);
                Session session = new Session(connection);
                ReceiverLink biReceiver = new ReceiverLink(session, subscriptionName, CreateSharedDurableSubscriberSource("orders", null), null);

                // wait for messages
                while (true)
                {
                    LogReceivedMessage(biReceiver);            
                }
            }

            {
                // sleep to make sure consumers have registered before sending messages
                Console.WriteLine("Sending messages...");
                Thread.Sleep(TimeSpan.FromSeconds(3));
                Connection connection = new Connection(peerAddr);
                Session session = new Session(connection);
                SenderLink sender = new SenderLink(session, "sender", ADDRESS);

                // continuously send messages
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    sender.Send(new Message("order placed at " + DateTime.Now.ToString()));
                }
            }

=======
            string url = (args.Length > 0) ? args[0] : "amqp://localhost:5672";
            int count = (args.Length > 2) ? Convert.ToInt32(args[2]) : 10;

            Address peerAddr = new Address(url);                 
            Connection connection = new Connection(peerAddr);
            Session session = new Session(connection);
                                   
            ReceiverLink biReceiver = new ReceiverLink(session, "BI|proxy1", CreateSubscriberSource("orders", null), null);
            SenderLink sender = new SenderLink(session, "sender", "orders");

            Connection connection2 = new Connection(peerAddr);
            Session session2 = new Session(connection2);
            ReceiverLink biReceiver2 = new ReceiverLink(session2, "BI|proxy2", CreateSubscriberSource("orders", null), null);
            biReceiver.Start(1);
            biReceiver2.Start(1);

            Message message = new Message("order placed for store 123");
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties["store"] = 123;
            sender.Send(message);   
            message = new Message("order placed for store 456");
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties["store"] = 456;
            sender.Send(message);
            message = new Message("order placed for store 456 again");
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties["store"] = 456;
            sender.Send(message);                                          

            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(biReceiver);

            sender.Close();    
            biReceiver.Close();                                                
            biReceiver2.Close();                                                
            session.Close();
            connection.Close();      
>>>>>>> bf5cea27167764658a25510b8feb628e2dc8d726
        }
        private static void LogReceivedMessage(ReceiverLink receiver)
        {
<<<<<<< HEAD
            Message msg = receiver.Receive(TimeSpan.FromSeconds(1));
            if (msg != null)
            {
                receiver.Accept(msg);
                Console.WriteLine(receiver.Name + " received: " + msg.Body.ToString()); 
            }  
=======
            Message msg = receiver.Receive(TimeSpan.FromSeconds(1));            
            if (msg != null) 
            {
                receiver.Accept(msg);
                Console.WriteLine(receiver.Name + " received: " + msg.Body.ToString());   
            }
>>>>>>> bf5cea27167764658a25510b8feb628e2dc8d726
        }

        private static Source CreateSharedDurableSubscriberSource(String address, List<String> filters)
        {
<<<<<<< HEAD
            Source source = new Source();
            source.Address = address;
=======
                Source source = new Source();
                source.Address = target;
                source.ExpiryPolicy = new Symbol("never");
                // Terminus Durability 0,1,2 (none, configuration, unsettled-state)
                source.Durable = 2;
                source.Capabilities = new Symbol[]{"topic", "shared", "global"};
                source.DistributionMode = new Symbol("copy");
                Map filterMap = new Map();
>>>>>>> bf5cea27167764658a25510b8feb628e2dc8d726

            // this source won't expire
            source.ExpiryPolicy = new Symbol("never");
            
            // maximum terminus durability 0,1,2 (none, configuration, unsettled-state)
            source.Durable = 2;

            // request capabilities "topic" (multicast in AMQ7 language), "shared" and "global"
            source.Capabilities = new Symbol[]{"topic", "shared", "global"};
            source.DistributionMode = new Symbol("copy");
            Map filterMap = new Map();

            // add filters, if any exist
            if (filters != null)
            {                    
                filters.ForEach(filterExpression => filterMap.Add(new Symbol(filterExpression), 
                    new DescribedValue(new Symbol("apache.org:selector-filter:string"), 
                    filterExpression)));
            }
            source.FilterSet = filterMap;
            return source;
        }
    }
}
