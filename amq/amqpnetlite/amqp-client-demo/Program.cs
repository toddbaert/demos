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
            string url = (args.Length > 0) ? args[0] : "amqp://guest:guest@127.0.0.1:5672";
            int count = (args.Length > 2) ? Convert.ToInt32(args[2]) : 10;

            Address peerAddr = new Address(url);                 
            Connection connection = new Connection(peerAddr);                   
            Session session = new Session(connection);
                        
            SenderLink send1 = new SenderLink(session, "sender1", "orders.s123");
            SenderLink send2 = new SenderLink(session, "sender2", "orders.s456");
            
            ReceiverLink biReceiver = new ReceiverLink(session, "BI", "orders.#");
            ReceiverLink otherReceiver = new ReceiverLink(session, "other-service", "orders.*");
            ReceiverLink store123Receiver = new ReceiverLink(session, "s123", "orders.s123");
            ReceiverLink store456Receiver = new ReceiverLink(session, "s456", "orders.s456");

            send1.Send(new Message("order placed for store 123"));                                               
            send2.Send(new Message("order placed for store 456"));                                               
        
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(otherReceiver);
            LogReceivedMessage(biReceiver);
            LogReceivedMessage(otherReceiver);
            LogReceivedMessage(store123Receiver);
            LogReceivedMessage(store456Receiver);       
                    
            send1.Close();    
            send2.Close();    
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
                source.DistributionMode = new Symbol("copy");
                return source;
        }
    }
}
