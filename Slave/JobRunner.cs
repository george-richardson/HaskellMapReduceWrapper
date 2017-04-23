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
            using (var proc = GenerateProcess(type))
            {
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
            using (var proc = GenerateProcess(type))
            {
                DateTime start = DateTime.Now;
                proc.Start();
                Task<string> outputTask = proc.StandardOutput.ReadToEndAsync();
                Console.WriteLine($"{type} started.");

                using (var stdIn = proc.StandardInput) 
                    stdIn.Write(input);

                proc.WaitForExit();
                DateTime end = DateTime.Now;
                Console.WriteLine($"{type} finished in {(end-start).TotalSeconds} seconds.");
                return outputTask.Result;
            }
        }

        private static Process GenerateProcess(JobType type) {
            return new Process() { StartInfo = GenerateProcessStartInfo(type) };
        }

        private static ProcessStartInfo GenerateProcessStartInfo(JobType type) {
            return new ProcessStartInfo
                {
                    FileName = "JobExecutable",
                    Arguments = type == JobType.Map ? "map" : "reduce",
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                };
        }
    }

    public enum JobType
    {
        Map, Reduce
    }
}
