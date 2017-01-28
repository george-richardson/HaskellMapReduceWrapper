using System;
using System.Diagnostics;
using System.Linq;

namespace MapReduceWrapper
{
    public static class Stack
    {
        public static void New()
        {
            RunStack(new[] {"new", "MapReduce"});
        }

        public static void Build()
        {
            RunStack(new []{"build"});
        }

        private static void RunStack(string[] args)
        {
            using (var proc = new Process())
            {
                proc.StartInfo = new ProcessStartInfo { FileName = "stack.exe", Arguments = args.Aggregate((s, s1) => $"{s} {s1}") };
                proc.Start();
                proc.WaitForExit();

                if (proc.ExitCode != 0)
                {
                    Environment.Exit(proc.ExitCode);
                }
            }
        }
    }
}
