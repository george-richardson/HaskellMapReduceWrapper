using System;
using System.Diagnostics;
using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MapReduceWrapper.NodeMode
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit http://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            app.Run(async (context) =>
            {
                if (context.Request.Path == "/load" && context.Request.Method == "POST")
                {
                    Console.WriteLine("Received new Job");
                    using (var stream = File.OpenWrite("JobExecutable"))
                    {
                        context.Request.Body.CopyTo(stream);
                        stream.Flush();
                    }
                    Console.WriteLine("Saved job file");
                    Console.WriteLine("Setting execute permission on job file");
                    using (var proc = new Process())
                    {
                        proc.StartInfo = new ProcessStartInfo
                        {
                            FileName = "chmod",
                            Arguments = "+x JobExecutable"
                        };
                        proc.Start();
                        proc.WaitForExit();
                    }
                    Console.WriteLine("Job loading complete");
                }
                else if (context.Request.Path == "/map" && context.Request.Method == "POST")
                {
                    string output;
                    Console.WriteLine("Received map command");
                    Console.WriteLine("Starting job process");

                    using (var proc = new Process ())
                    {
                        proc.StartInfo = new ProcessStartInfo
                        {
                            FileName = "JobExecutable",
                            Arguments = "map",
                            RedirectStandardInput = true,
                            RedirectStandardOutput = true,
                        };
                        proc.Start();
                        Console.WriteLine("Map started");

                        using (var stdIn = proc.StandardInput)
                        {
                            stdIn.Write(new StreamReader(context.Request.Body).ReadToEnd());
                        }

                        proc.WaitForExit();
                        Console.WriteLine("Map finished. Output: ");
                        output = proc.StandardOutput.ReadToEnd();
                        Console.WriteLine(output);
                    }
                    await context.Response.WriteAsync(output);
                }
                else if (context.Request.Path == "/map" && context.Request.Method == "POST")
                {
                    string output;
                    Console.WriteLine("Received reduce command");
                    Console.WriteLine("Starting job process");

                    using (var proc = new Process())
                    {
                        proc.StartInfo = new ProcessStartInfo
                        {
                            FileName = "JobExecutable",
                            Arguments = "reduce",
                            RedirectStandardInput = true,
                            RedirectStandardOutput = true,
                        };
                        proc.Start();
                        Console.WriteLine("Reduce started");

                        using (var stdIn = proc.StandardInput)
                        {
                            stdIn.Write(new StreamReader(context.Request.Body).ReadToEnd());
                        }

                        proc.WaitForExit();
                        Console.WriteLine("Reduce finished. Output: ");
                        output = proc.StandardOutput.ReadToEnd();
                        Console.WriteLine(output);
                    }
                    await context.Response.WriteAsync(output);
                }
                else if (context.Request.Path == "/ping")
                {
                    Console.WriteLine("Ping received");
                    await context.Response.WriteAsync("Pong");
                }
                else
                {
                    await context.Response.WriteAsync("Map Reduce Node Server");
                }
            });
        }
    }
}