using System;
using System.Collections.Generic;
using System.Threading;
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
            string url = (args.Length > 0) ? args[0] : "amqp://localhost:61616";
            int count = (args.Length > 2) ? Convert.ToInt32(args[2]) : 10;

            Address peerAddr = new Address(url);                 
            Connection connection = new Connection(peerAddr, SaslProfile.Anonymous, new Open() { ContainerId = "proxy1" }, null);
            //Connection connection = new Connection(peerAddr);
            Session session = new Session(connection);
                       
            
            ReceiverLink biReceiver = new ReceiverLink(session, "BI", CreateSubscriberSource("orders", null), null);
            ReceiverLink otherReceiver = new ReceiverLink(session, "other-service", CreateSubscriberSource("orders", null), null);
            ReceiverLink store123Receiver = new ReceiverLink(session, "s123", CreateSubscriberSource("orders", new List<string>(){"store = 123"}), null);
            ReceiverLink store456Receiver = new ReceiverLink(session, "s456", CreateSubscriberSource("orders", new List<string>(){"store = 456"}), null);
            SenderLink sender = new SenderLink(session, "sender", "orders");



            Connection connection2 = new Connection(peerAddr, SaslProfile.Anonymous, new Open() { ContainerId = "proxy1" }, null);
            //Connection connection2 = new Connection(peerAddr);
            Session session2 = new Session(connection2);
            ReceiverLink biReceiver2 = new ReceiverLink(session2, "BI", CreateSubscriberSource("orders", null), null);
            //biReceiver2.Start(1);

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

            //Thread.Sleep(TimeSpan.FromSeconds(30));


            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver2);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(biReceiver);



            // LogReceivedMessage();
            // LogReceivedMessage();




            sender.Close();    
            store123Receiver.Close();         
            store456Receiver.Close();                                       
            biReceiver.Close();                                                
            //biReceiver2.Close();                                                
            session.Close();
            connection.Close();      
        }

        private static void LogReceivedMessage(ReceiverLink receiver)
        {
            Message msg = receiver.Receive();
            receiver.Accept(msg);
            Console.WriteLine(receiver.Name + " received: " + msg.Body.ToString());   
        }

        private static Source CreateSubscriberSource(String target, List<String> filters)
        {
                Source source = new Source();
                source.Address = target;
                source.ExpiryPolicy = new Symbol("never");
                // Fields fields = new Fields();
                // fields.Add(new Symbol("supported-dist-modes"), new Symbol("copy"));
                // source.DynamicNodeProperties = fields;
                
                // Terminus Durability 0,1,2 (none, configuration, unsettled-state)
                source.Durable = 2;
                source.Capabilities = new Symbol[3]{"topic", "shared", "global"};
                source.DistributionMode = new Symbol("copy");
                Map filterMap = new Map();

                if (filters != null)
                {                    
                    filters.ForEach(x => filterMap.Add(new Symbol(x), new DescribedValue(new Symbol("apache.org:selector-filter:string"), x)));
                }
                source.FilterSet = filterMap;
                return source;
        }
    }
}
