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

        }
        private static void LogReceivedMessage(ReceiverLink receiver)
        {
            Message msg = receiver.Receive(TimeSpan.FromSeconds(1));
            if (msg != null)
            {
                receiver.Accept(msg);
                Console.WriteLine(receiver.Name + " received: " + msg.Body.ToString()); 
            }  
        }

        private static Source CreateSharedDurableSubscriberSource(String address, List<String> filters)
        {
            Source source = new Source();
            source.Address = address;

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
