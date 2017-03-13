using System;
using System.Linq;
using MapReduceWrapper.Master;
using MapReduceWrapper.Slave;

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
                        switch (args.First())
                        {
                            case "node":
                                //Run in node mode.
                                StartNode();
                                break;
                            case "new":
                                //Create new environment.
                                Configuration.New();
                                break;
                            case "build":
                                //Build the job
                                Configuration.Build();
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
                                        Configuration.Ping();
                                        break;
                                    case "print":
                                        Configuration.Print();
                                        break;
                                    default:
                                        PrintArgErr();
                                        break;
                                }
                                break;
                            case "run":
                                //Build and execute the job.
                                Master.Master.Run(args[1]);
                                break;
                            case "exec":
                                Master.Master.Exec(args[1]);
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

        static void StartNode()
        {
            Console.WriteLine("Starting in node mode. Ctrl+C to stop.");
            NodeServer.Start();
        }
    }
}

