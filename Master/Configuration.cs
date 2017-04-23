using System;
using System.Linq;
using MapReduceWrapper.External;

namespace MapReduceWrapper.Master
{
    public static class Configuration
    {
        private static readonly Git Git = new Git();
        private static readonly Stack Stack = new Stack();

        public static void New()
        {
            Git.Clone();
            Stack.Setup();
        }

        public static void Build()
        {
            Stack.Build();
        }

        public static void Install()
        {
            Stack.Install();
        }

        public static void BuildInstall()
        {
            Build();
            Install();
        }

        public static void Ping()
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

        public static void Print()
        {
            var cluster = new Cluster.Cluster();
            foreach (var node in cluster.Manifest)
            {
                Console.WriteLine($"{node.IP}:{node.Port}");
            }
        }
    }
}
