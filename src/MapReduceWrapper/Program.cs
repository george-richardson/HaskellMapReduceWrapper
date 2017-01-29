using System;
using System.Linq;
using MapReduceWrapper.External;
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
                            new Git().Clone();
                            new Stack().Setup();
                            break;
                        case "run":
                            //Build and execute the job.
                            var stack = new Stack();
                            stack.Build();
                            stack.Install();
                            new Cluster.Cluster().LoadProgram("HaskellMapReduce-exe");
                            //Send the files to the cluster
                            break;
                        case "build":
                            //Build the job
                            new Stack().Build();
                            break;
                        case "exec":
                            //Execute precompiled job
                            new Stack().Install();
                            //Send the files to the cluster
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
                                case "print":
                                    PrintCluster();
                                    break;
                                default:
                                    PrintArgErr();
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

        static void PrintCluster()
        {
            var cluster = new Cluster.Cluster();
            foreach (var ip in cluster.Manifest)
            {
                Console.WriteLine(ip);
            }
        }

        static void StartNode()
        {
            Console.WriteLine("Starting in node mode. Ctrl+C to stop.");
            NodeServer.Start();
        }
    }
}

