using System;
using System.Diagnostics;
using System.Linq;

namespace MapReduceWrapper.External
{
    public abstract class External
    {
        private readonly string _executableName;

        protected External(string executableName)
        {
            _executableName = executableName;
        }

        protected void Run(string[] args)
        {
            using (var proc = new Process())
            {
                proc.StartInfo = new ProcessStartInfo
                {
                    FileName = _executableName,
                    Arguments = args.Aggregate((s, s1) => $"{s} {s1}")
                };
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
