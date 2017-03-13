using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using MapReduceWrapper.Cluster;
using MapReduceWrapper.Cluster.Transport;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedParameter.Global
// ReSharper disable ClassNeverInstantiated.Global

namespace MapReduceWrapper.Slave
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
                        Console.WriteLine("Received map command");

                        DataStore.SetData(
                            JsonConvert.DeserializeObject<Dictionary<dynamic, List<dynamic>>>(JobRunner.Run(
                                JobType.Map, context.Request.Body)));

                        Console.WriteLine($"{DataStore.Count()} keys.");

                        await context.Response.WriteAsync(JsonConvert.SerializeObject(DataStore.GetKeyCounts()));
                    }
                    else if (context.Request.Path == "/reduce" && context.Request.Method == "POST")
                    {
                        Console.WriteLine("Received reduce command");
                        Console.WriteLine("Building job data.");
                        ReduceRequestJson requestJson;
                        using (var sr = new StreamReader(context.Request.Body))
                            requestJson = JsonConvert.DeserializeObject<ReduceRequestJson>(sr.ReadToEnd());

                        string requestContent = JsonConvert.SerializeObject(requestJson.Keys);
                        Cluster.Cluster.Validate(requestJson.Nodes);
                        var clusterDataRequests = requestJson.Nodes.ToDictionary(node => node, node => requestContent);
                        var clusterData = Cluster.Cluster.Post<Dictionary<dynamic, List<dynamic>>>("data",
                            clusterDataRequests);

                        Dictionary<dynamic, List<dynamic>> mapData = new Dictionary<dynamic, List<dynamic>>();
                        foreach (KeyValuePair<Node, Dictionary<dynamic, List<dynamic>>> clusterDatum in clusterData)
                        {
                            foreach (KeyValuePair<dynamic, List<dynamic>> pair in clusterDatum.Value)
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

                        var outputJson =
                            JsonConvert.DeserializeObject<Dictionary<dynamic, dynamic>>(JobRunner.Run(JobType.Reduce,
                                JsonConvert.SerializeObject(mapData)));

                        var result = new ReduceResponseJson()
                        {
                            Results = outputJson.Select(pair => new ReduceResponseJsonItem { Key = pair.Key.ToString(), Value = pair.Value.ToString() }).ToList()
                        };

                        
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