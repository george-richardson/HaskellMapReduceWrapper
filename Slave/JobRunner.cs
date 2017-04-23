using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace MapReduceWrapper.Slave
{
    public static class JobRunner
    {
        public static string Run(JobType type, Stream stream)
        {
            using (var proc = new Process())
            {
                proc.StartInfo = new ProcessStartInfo
                {
                    FileName = "JobExecutable",
                    Arguments = type == JobType.Map ? "map" : "reduce",
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                };
                DateTime start = DateTime.Now;
                proc.Start();
                Task<string> outputTask = proc.StandardOutput.ReadToEndAsync();
                Console.WriteLine($"{type} started.");

                using (var stdIn = proc.StandardInput.BaseStream) {
                    stream.CopyTo(stdIn);
                    stdIn.Flush();
                }

                proc.WaitForExit();
                DateTime end = DateTime.Now;
                Console.WriteLine($"{type} finished in {(end-start).TotalSeconds} seconds.");
                return outputTask.Result;
            }
        }

        public static string Run(JobType type, String input)
        {
            using (var sr = new StringReader(input))
                return Run(type, sr.ReadToEnd());
        }
    }

    public enum JobType
    {
        Map, Reduce
    }
}
