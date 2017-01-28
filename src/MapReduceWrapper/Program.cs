using System;
using System.Linq;
using MapReduceWrapper.NodeMode;

namespace MapReduceWrapper
{
    public class Program
    {
        public static void Main(string[] args)
        {
            switch (args.Length)
            {
                case 0:
                    Console.WriteLine("No arguments given. Type --help for help.");
                    Environment.Exit(1);
                    break;
                case 1:
                    switch (args.First())
                    {
                        case "node":
                            //Run in node mode.
                            StartNode();
                            break;
                        case "new":
                            //Create new environment.
                            Stack.New();
                            break;
                        case "run":
                            //Build and execute the job.
                            break;
                        case "build":
                            //Build the job
                            break;
                        case "exec":
                            //Execute precompiled job
                            break;
                        case "--help":
                            PrintHelp();
                            break;
                        default:
                            PrintArgErr();
                            break;
                    }
                    break;
                case 2:
                    switch (args.First())
                    {
                        case "cluster":
                            switch (args[1])
                            {
                                case "ping":
                                    PingCluster();
                                    break;
                            }
                            break;
                        default:
                            PrintArgErr();
                            break;
                    }
                    break;
            }
        }

        static void PrintArgErr()
        {
            Console.WriteLine("Unknown arguments. Type --help for help.");
            Environment.Exit(1);
        }

        static void PrintHelp()
        {
            Console.WriteLine("Help Text"); //todo fill this out
            Environment.Exit(0);
        }

        static void PingCluster()
        {
            Console.WriteLine("Pinging nodes...");
            
            var cluster = new Cluster.Cluster();
            var results = cluster.Test();

            Console.WriteLine();
            foreach (var result in results)
            {
                Console.WriteLine($"{result.Key} {(result.Value ? "Up" : "Down")}");
            }
            Environment.Exit(results.Any(pair => !pair.Value) ? 1 : 0);
        }

        static void StartNode()
        {
            Console.WriteLine("Starting in node mode. Ctrl-C to stop.");
            NodeServer.Start();
        }
    }
}

