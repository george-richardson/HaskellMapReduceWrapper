using System;
using System.Linq;
using MapReduceWrapper.External;
using MapReduceWrapper.NodeMode;

namespace MapReduceWrapper
{
    // ReSharper disable once UnusedMember.Global
    public class Program
    {
        // ReSharper disable once UnusedMember.Global
        public static void Main(string[] args)
        {
            try
            {
                switch (args.Length)
                {
                    case 0:
                        Console.WriteLine("No arguments given. Type --help for help.");
                        Environment.Exit(1);
                        break;
                    case 1:
                        Cluster.Cluster cluster;
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
                            case "build":
                                //Build the job
                                new Stack().Build();
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
                            case "run":
                                //Build and execute the job.
                                var stack = new Stack();
                                stack.Build();
                                stack.Install();
                                cluster = new Cluster.Cluster();
                                cluster.LoadProgram("HaskellMapReduce-exe");
                                cluster.ExecuteProgram(args[1]);
                                break;
                            case "exec":
                                cluster = new Cluster.Cluster();
                                cluster.LoadProgram("HaskellMapReduce-exe");
                                cluster.ExecuteProgram(args[1]);
                                break;
                            default:
                                PrintArgErr();
                                break;
                        }
                        break;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                Environment.Exit(1);
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
            foreach (var node in cluster.Manifest)
            {
                Console.WriteLine($"{node.IP}:{node.Port}");
            }
        }

        static void StartNode()
        {
            Console.WriteLine("Starting in node mode. Ctrl+C to stop.");
            NodeServer.Start();
        }
    }
}

