using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using MapReduceWrapper.Cluster.Transport;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

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
                try
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

                        using (var proc = new Process())
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
                            DataStore.SetData(JsonConvert.DeserializeObject<Dictionary<dynamic, List<dynamic>>>(output));
                            Console.WriteLine(output);
                        }
                        await context.Response.WriteAsync(JsonConvert.SerializeObject(DataStore.GetKeyCounts()));
                    }
                    else if (context.Request.Path == "/reduce" && context.Request.Method == "POST")
                    {
                        string output;
                        Console.WriteLine("Received reduce command");
                        Console.WriteLine("Building job data.");
                        ReduceRequestJson requestJson;
                        using (var sr = new StreamReader(context.Request.Body))
                            requestJson = JsonConvert.DeserializeObject<ReduceRequestJson>(sr.ReadToEnd());

                        List<Task<HttpResponseMessage>> dataTasks =
                            requestJson.Nodes.Select(
                                    s =>
                                        Cluster.Cluster.GetClient(IPAddress.Parse(s), 80)
                                            .PostAsync("data", new StringContent(JsonConvert.SerializeObject(requestJson.Keys))))
                                .ToList();
                        Task.WaitAll(dataTasks.Cast<Task>().ToArray());

                        Dictionary<dynamic, List<dynamic>> mapData = new Dictionary<dynamic, List<dynamic>>();
                        foreach (Task<HttpResponseMessage> dataTask in dataTasks)
                        {
                            if (!dataTask.Result.IsSuccessStatusCode)
                            {
                                throw new Exception("Node fault");
                            }

                            Dictionary<dynamic, List<
                            dynamic >> nodeData =
                                JsonConvert.DeserializeObject<Dictionary<dynamic, List<dynamic>>>(
                                    await dataTask.Result.Content.ReadAsStringAsync());

                            foreach (KeyValuePair<dynamic, List<dynamic>> pair in nodeData)
                            {
                                if (!mapData.ContainsKey(pair.Key))
                                {
                                    mapData.Add(pair.Key, pair.Value);
                                }
                                else
                                {
                                    ((List<dynamic>)mapData[pair.Key]).AddRange(pair.Value);
                                }
                            }
                        }

                        Console.WriteLine("Starting job process");
                        ReduceResponseJson result;
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
                                stdIn.Write(JsonConvert.SerializeObject(mapData));
                            }

                            proc.WaitForExit();
                            Console.WriteLine("Reduce finished. Output: ");
                            output = proc.StandardOutput.ReadToEnd();
                            Console.WriteLine(output);
                            var outputJson = JsonConvert.DeserializeObject<Dictionary<dynamic, dynamic>>(output);
                            result = new ReduceResponseJson()
                            {
                                Results = outputJson.Select(pair => new ReduceResponseJsonItem { Key = pair.Key, Value = pair.Value }).ToList()
                            };
                        }
                        await context.Response.WriteAsync(JsonConvert.SerializeObject(result));
                    }
                    else if (context.Request.Path == "/data" && context.Request.Method == "POST")
                    {
                        Console.WriteLine("Data request. ");
                        List<dynamic> keys =
                            JsonConvert.DeserializeObject<List<dynamic>>(new StreamReader(context.Request.Body).ReadToEnd());
                        await context.Response.WriteAsync(JsonConvert.SerializeObject(DataStore.GetData(keys)));
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
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.Message);
                    Console.Error.WriteLine(e.StackTrace);
                    throw;
                }
            });
        }
    }
}