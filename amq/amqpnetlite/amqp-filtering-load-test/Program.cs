/*
This is a quick testing script designed to investigate the performance characteristics of a single topic, 
with many subscribers each with a different filter. First, a number of subscrubers is created (arg1), 
then a specified number of messages (arg2) are braodcasted to all but a specified number of "benchmark"
subscribers (arg3). Once the queues are filled with the broadcast messages, we send and receive specific
messages to our benchmark subscribers and time the round trip.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Transactions;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;

namespace Amqp.Testing
{
    class Program
    {
        static void Main(string[] args)
        {
            // number of queues (subscrubers) to be created
            int numQueues = Int32.Parse(args[0]);
            // number of broadcast messages to send
            int numMessages = Int32.Parse(args[1]);
            // number of benchmark subs to identify to sample send/recive speed.
            int numBenchmarks = Int32.Parse(args[2]);
            // broker URL 
            String url = (args.Length > 3) ? args[3] : "amqp://localhost:5672";
            // topic name
            String address = (args.Length > 4) ? args[4] : "orders";
            
            Address peerAddr = new Address(url);                            
            Connection connection = new Connection(peerAddr);
            //Connection connection = new Connection(peerAddr, SaslProfile.Anonymous, new Open { ContainerId = null }, null);
            Session session = new Session(connection);
            ReceiverLink[] receivers = new ReceiverLink[numQueues];
            Random random = new Random();
            List<int> benchmarkIndicies = new List<int>();
            Stopwatch stopwatch = new Stopwatch();
            
            // populate array of random benchmarks, these won't receive broadcast messages
            while (benchmarkIndicies.Count < numBenchmarks)
            {       
                int nextRandom = random.Next(0, numQueues - 1);       
                if (!benchmarkIndicies.Contains(nextRandom))
                {
                    benchmarkIndicies.Add(nextRandom);
                }                
            }

            Console.WriteLine("Creating subscribers...");
            for (var i = 0; i < numQueues; i++)
            {
                Console.Write("\rCreating subscriber " + (i + 1) + "/" + numQueues);
                if (!benchmarkIndicies.Contains(i))
                {
                    // create general subscribers, listening to broadcast messages and store-specific messages
                    receivers[i] = new ReceiverLink(session, "sub" + i, CreateSharedDurableSubscriberSource(address, new List<String>(){ "store=" + i + " OR allstores=true" }), null);
                }
                else
                {
                    // create benchmark subscribers, listening for only their store number
                    receivers[i] = new ReceiverLink(session, "sub" + i, CreateSharedDurableSubscriberSource(address, new List<String>(){ "store=" + i }), null);
                }                
                Thread.Sleep(TimeSpan.FromSeconds(.0001));
            }
            Console.WriteLine("\nDone!");

            SenderLink sender = new SenderLink(session, "sender", address);

            // send broadcast messages
            Console.WriteLine("Sending broadcast messages...");
            stopwatch.Start();
            for (var i = 0; i < numMessages; i++)
            {
                Console.Write("\rSending message " + (i + 1) + "/" + numMessages);
                
                Message message = new Message(Guid.NewGuid().ToString());
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties["allstores"] = true;
                message.Header = new Header();
                message.Header.Durable = true;

                try
                {
                    sender.Send(message);
                }
                catch
                {
                    Console.WriteLine("ERROR sending braodcast message");
                }   
            }
            stopwatch.Stop();
            Console.WriteLine("\n" + numMessages + " messages broadcasted in " + stopwatch.ElapsedMilliseconds + "ms!");
            stopwatch.Reset();
        
            Console.WriteLine("Sending messages to randomly selected benchmark subs...");

            // send messages to our random benchmarks, tracking delta between send/receive
            benchmarkIndicies.ForEach(i => {
                Message benchmarkMessage = new Message("benchmark message " + i);
                benchmarkMessage.ApplicationProperties = new ApplicationProperties();
                benchmarkMessage.ApplicationProperties["store"] = i;        
                benchmarkMessage.Header = new Header();
                benchmarkMessage.Header.Durable = true;   
                stopwatch.Start();
                sender.Send(benchmarkMessage);
                
                Message receivedMessage = receivers[i].Receive(TimeSpan.FromSeconds(60));
                receivers[i].Accept(receivedMessage);
                stopwatch.Stop();
                Console.WriteLine("Benchmark " + receivers[i].Name + " received: \"" + receivedMessage.Body + "\" in " + stopwatch.ElapsedMilliseconds + "ms!");
                stopwatch.Reset();
                Thread.Sleep(TimeSpan.FromSeconds(1));
            });
            Console.WriteLine("Receiving broadcast message from random subs...");
            
            for (var i = 0; i < numMessages; i++)
            {                                 
                {
                    int nextRandom = random.Next(0, numQueues - 1);       
                    if (!benchmarkIndicies.Contains(nextRandom))
                    {
                        Message receivedMessage = receivers[nextRandom].Receive(TimeSpan.FromSeconds(60));
                        stopwatch.Start();
                        receivers[nextRandom].Accept(receivedMessage);
                        stopwatch.Stop();
                        Console.WriteLine(receivers[nextRandom].Name + " received broadcast message: \"" + receivedMessage.Body + "\" in " + stopwatch.ElapsedMilliseconds + "ms!");
                        Thread.Sleep(TimeSpan.FromSeconds(1));
                    }
                }                                
                stopwatch.Reset();                
            }
        }        

        private static Source CreateSharedDurableSubscriberSource(String address, List<String> filters = null)
        {
            Source source = new Source();
            source.Address = address;

            // this source won't expire
            source.ExpiryPolicy = new Symbol("never");
            
            // maximum terminus durability 0,1,2 (none, configuration, unsettled-state)
            source.Durable = 2;

            // request capabilities "topic" (multicast in AMQ7 language), "global" and "shared"
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
            //source.Timeout
            return source;
        }
    }
}
