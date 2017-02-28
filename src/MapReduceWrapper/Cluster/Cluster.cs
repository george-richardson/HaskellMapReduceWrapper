using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using MapReduceWrapper.Cluster.Exceptions;
using MapReduceWrapper.Cluster.Transport;
using Newtonsoft.Json;

namespace MapReduceWrapper.Cluster
{
    public class Cluster
    {
        private readonly NodeManifest _manifest;

        public NodeManifest Manifest => _manifest;

        public Cluster() : this(NodeManifest.Load("node.manifest"))
        {

        }

        public Cluster(string path) : this(NodeManifest.Load(path))
        {

        }

        private Cluster(NodeManifest manifest)
        {
            _manifest = manifest;
        }

        public TestResults Test()
        {
            return new TestResults(_manifest.ToDictionary(address => address, Ping));
        }

        public void LoadProgram(string path)
        {
            using (var stream = File.OpenRead(path))
            {
                foreach (IPAddress address in _manifest)
                {
                    using (Stream buffer = new MemoryStream())
                    {
                        stream.CopyTo(buffer);
                        buffer.Seek(0, SeekOrigin.Begin);
                        WaitForSuccess(GetClient(address, 80).PostAsync("load", new StreamContent(buffer)), address);
                    }

                    stream.Seek(0, SeekOrigin.Begin);
                }
            }
        }

        public async void ExecuteProgram(string path)
        {
            try
            {
                //Load and split file.
                Console.WriteLine("Splitting");
                FileSplitter splitter = new FileSplitter(path, _manifest.Count());

                //Run the map.
                Console.WriteLine("Mapping");
                Dictionary<IPAddress, Task<HttpResponseMessage>> mapTasks =
                    new Dictionary<IPAddress, Task<HttpResponseMessage>>();
                foreach (IPAddress address in _manifest)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var line in splitter.TakeOne())
                    {
                        sb.Append(line);
                        sb.Append('\n');
                    }
                    mapTasks.Add(address, GetClient(address, 80).PostAsync("map", new StringContent(sb.ToString())));
                }

                //Wait for maps to finish
                Task.WaitAll(mapTasks.Values.Cast<Task>().ToArray());

                //Co-ordinate reduce
                Dictionary<string, int> keyCounts = new Dictionary<string, int>();
                foreach (KeyValuePair<IPAddress, Task<HttpResponseMessage>> pair in mapTasks)
                {
                    if (!pair.Value.Result.IsSuccessStatusCode)
                    {
                        throw new Exception("Node fault");
                    }

                    var json =
                        JsonConvert.DeserializeObject<MapResponseJson>(
                            await pair.Value.Result.Content.ReadAsStringAsync());

                    foreach (MapResponseJsonItem keyCount in json.Keys)
                    {
                        if (keyCounts.ContainsKey(keyCount.Key))
                        {
                            keyCounts[keyCount.Key] += keyCount.Count;
                        }
                        else
                        {
                            keyCounts.Add(keyCount.Key, keyCount.Count);
                        }
                    }
                }

                Dictionary<IPAddress, KeysCount> nodeCounts = _manifest.ToDictionary(address => address,
                    address => new KeysCount());
                foreach (KeyValuePair<string, int> key in keyCounts)
                {
                    nodeCounts.Aggregate(
                            (minNode, node) =>
                                minNode.Equals(default(KeyValuePair<IPAddress, KeysCount>))
                                    ? node
                                    : node.Value.TotalCount < minNode.Value.TotalCount ? node : minNode)
                        .Value.Add(key.Key, key.Value);
                }

                //Run reduce
                Console.WriteLine("Reducing");
                Dictionary<IPAddress, Task<HttpResponseMessage>> reduceTasks =
                    new Dictionary<IPAddress, Task<HttpResponseMessage>>();
                foreach (KeyValuePair<IPAddress, KeysCount> nodeCount in nodeCounts)
                {
                    reduceTasks.Add(nodeCount.Key, GetClient(nodeCount.Key, 80)
                        .PostAsync("reduce",
                            new StringContent(
                                JsonConvert.SerializeObject(new ReduceRequestJson
                                {
                                    Keys = nodeCount.Value.Keys,
                                    Nodes = _manifest.Select(address => address.ToString()).ToList()
                                }))));
                }
                //Wait for reduces to finish
                Task.WaitAll(reduceTasks.Values.Cast<Task>().ToArray());

                //Get and compile results.
                Console.WriteLine("Compiling results");
                StringBuilder builder = new StringBuilder();
                foreach (KeyValuePair<IPAddress, Task<HttpResponseMessage>> reduceTask in reduceTasks)
                {
                    string response = await reduceTask.Value.Result.Content.ReadAsStringAsync();
                    ReduceResponseJson responseJson = JsonConvert.DeserializeObject<ReduceResponseJson>(response);
                    foreach (var item in responseJson.Results)
                    {
                        builder.Append($"{item.Key} {item.Value}\n");
                    }
                }
                File.WriteAllText("output.txt", builder.ToString());
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                throw;
            }

        }

        private bool Ping(IPAddress address)
        {
            bool result = false;
            try
            {
                var response = GetClient(address, 80).GetAsync("ping");
                response.Wait();
                result = response.Result.IsSuccessStatusCode;
            }
            catch
            {
                //ignore
            }

            return result;
        }

        public static HttpClient GetClient(IPAddress address, int port)
        {
            string uri = $"http://{address}:{port}/";
            HttpClient client = new HttpClient
            {
                BaseAddress = new Uri(uri)
            };
            return client;
        }

        private void WaitForSuccess(Task<HttpResponseMessage> responseTask, IPAddress address)
        {
            responseTask.Wait();
            if (!responseTask.Result.IsSuccessStatusCode)
            {
                throw new NodeException(address);
            }
        }
    }
}
