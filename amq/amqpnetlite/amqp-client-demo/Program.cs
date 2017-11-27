using System;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace amqp_client_demo
{
    class Program
    {
        static void Main(string[] args)
        {
            string url = (args.Length > 0) ? args[0] : "amqp://localhost:5672";
            int count = (args.Length > 2) ? Convert.ToInt32(args[2]) : 10;

            Address peerAddr = new Address(url);                 
            Connection connection = new Connection(peerAddr);                   
            Session session = new Session(connection);

            Map filters = new Map();
            filters.Add(new Symbol("store123"), new DescribedValue(new Symbol("apache.org:selector-filter:string"), "store = 123"));
            Source ordersSource123 = new Source(){ Address = "orders", FilterSet = filters };
            ordersSource123.DistributionMode = new Symbol("move");
            
            filters = new Map();
            filters.Add(new Symbol("store456"), new DescribedValue(new Symbol("apache.org:selector-filter:string"), "store = 456"));
            Source ordersSource456 = new Source(){ Address = "orders", FilterSet = filters };
            ordersSource456.DistributionMode = new Symbol("move");

                        
            SenderLink sender = new SenderLink(session, "sender", "orders");
            
            ReceiverLink biReceiver = new ReceiverLink(session, "BI", "orders");
            ReceiverLink otherReceiver = new ReceiverLink(session, "other-service", "orders");
            ReceiverLink store123Receiver = new ReceiverLink(session, "s123", ordersSource123, null);
            ReceiverLink store456Receiver = new ReceiverLink(session, "s456", ordersSource456, null);

            Message message = new Message("order placed for store 123");
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties["store"] = 123;
            sender.Send(message);   
            message = new Message("order placed for store 456");
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties["store"] = 456;
            sender.Send(message);                                               
        
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(otherReceiver);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(otherReceiver);
            LogReceivedMessage(store123Receiver);
            LogReceivedMessage(store456Receiver);       
                    
            sender.Close();    
            store123Receiver.Close();         
            store456Receiver.Close();                                       
            biReceiver.Close();                                                
            session.Close();
            connection.Close();      
        }

        private static void LogReceivedMessage(ReceiverLink receiver)
        {
            Message msg = receiver.Receive();
            receiver.Accept(msg);
            Console.WriteLine(receiver.Name + " received: " + msg.Body.ToString());   
        }

        private static Source CreateSubscriberSource(String target)
        {
                Source source = new Source();
                source.Address = target;
                source.ExpiryPolicy = new Symbol("never");

                // Terminus Durability 0,1,2 (none, configuration, unsettled-state)
                source.Durable = 2;
                source.DistributionMode = new Symbol("move");
                return source;
        }
    }
}
