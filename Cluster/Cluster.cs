using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private readonly List<Node> _manifest;

        internal List<Node> Manifest => _manifest;

        public Cluster()
        {
            _manifest = LoadFromPath("node.manifest");
        }

        public Dictionary<Node, bool> Test()
        {
            return
                Get("ping", _manifest)
                    .ToDictionary(pair => pair.Key, pair => pair.Value.IsSuccessStatusCode);
        }

        public void LoadProgram(string path)
        {
            using (var stream = File.OpenRead(path))
            {
                foreach (Node node in _manifest)
                {
                    using (Stream buffer = new MemoryStream())
                    {
                        stream.CopyTo(buffer);
                        buffer.Seek(0, SeekOrigin.Begin);
                        WaitForSuccess(node.Client.PostAsync("load", new StreamContent(buffer)), node);
                    }

                    stream.Seek(0, SeekOrigin.Begin);
                }
            }
        }

        public void ExecuteProgram(string path)
        {
            DateTime start = DateTime.Now;

            //Load and split file.
            Console.WriteLine("Splitting");
            FileSplitter splitter = new FileSplitter(path, _manifest.Count);
            DateTime splitEnd = DateTime.Now;
            Console.WriteLine($"in {(splitEnd - start).TotalSeconds} seconds");

            //Run the map.
            Console.WriteLine("Mapping");
            Dictionary<Node, string> mapInputs = _manifest.ToDictionary(node => node, node =>
            {
                StringBuilder sb = new StringBuilder();
                foreach (var line in splitter.TakeOne())
                {
                    sb.Append(line);
                    sb.Append('\n');
                }
                return sb.ToString();
            });
            var mapResults = Post<MapResponseJson>("map", mapInputs);

            DateTime mapEnd = DateTime.Now;
            Console.WriteLine($"in {(mapEnd - splitEnd).TotalSeconds} seconds");

            //Co-ordinate reduce
            Dictionary<string, int> keyCounts = new Dictionary<string, int>();
            Console.WriteLine("Balancing reduce keys");
            foreach (KeyValuePair<Node, MapResponseJson> pair in mapResults)
            {
                foreach (MapResponseJsonItem keyCount in pair.Value.Keys)
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
            Dictionary<Node, KeysCount> nodeCounts = _manifest.ToDictionary(node => node,
                address => new KeysCount());
            Node minNode = _manifest.First();
            int minCount;
            foreach (KeyValuePair<string, int> key in keyCounts)
            {
                nodeCounts[minNode].Add(key.Key, key.Value);
                minCount = nodeCounts.Min(pair => pair.Value.TotalCount);
                minNode = nodeCounts.First(pair => pair.Value.TotalCount == minCount).Key;
            }
            Console.WriteLine($"{nodeCounts.Sum(pair => pair.Value.Keys.Count)} keys");

            DateTime balanceEnd = DateTime.Now;
            Console.WriteLine($"in {(balanceEnd - mapEnd).TotalSeconds} seconds");

            //Run reduce
            Console.WriteLine("Reducing");
            Dictionary<Node, ReduceResponseJson> responses = Post<ReduceResponseJson>("reduce",
                nodeCounts.ToDictionary(pair => pair.Key, pair => JsonConvert.SerializeObject(new ReduceRequestJson
                {
                    Keys = pair.Value.Keys,
                    Nodes = _manifest
                })));
            
            DateTime reduceEnd = DateTime.Now;
            Console.WriteLine($"in {(reduceEnd - balanceEnd).TotalSeconds} seconds");

            //Get and compile results.
            Console.WriteLine("Compiling results");
            StringBuilder builder = new StringBuilder();
            foreach (KeyValuePair<Node, ReduceResponseJson> reduceTask in responses)
            {
                foreach (var item in reduceTask.Value.Results)
                {
                    builder.Append($"{item.Key} {item.Value}\n");
                }
            }
            File.WriteAllText("output.txt", builder.ToString());
            
            DateTime end = DateTime.Now;
            Console.WriteLine($"Total: {(end - start).TotalSeconds} seconds");
        }

        private void WaitForSuccess(Task<HttpResponseMessage> responseTask, Node node)
        {
            responseTask.Wait();
            if (!responseTask.Result.IsSuccessStatusCode)
            {
                throw new NodeException(node);
            }
        }

        private static Dictionary<Node, HttpResponseMessage> Get(string uri, List<Node> nodes)
        {
            Dictionary<Node, Task<HttpResponseMessage>> tasks = nodes.ToDictionary(node => node,
                node => node.Client.GetAsync(uri));
            Task.WaitAll(tasks.Values.Cast<Task>().ToArray());
            return tasks.ToDictionary(pair => pair.Key, pair => pair.Value.Result);
        }

        private static Dictionary<Node, HttpResponseMessage> Post(string uri, Dictionary<Node, string> inputs)
        {
            Dictionary<Node, Task<HttpResponseMessage>> tasks = inputs.ToDictionary(pair => pair.Key,
                pair => pair.Key.Client.PostAsync(uri, new StringContent(pair.Value)));
            Task.WaitAll(tasks.Values.Cast<Task>().ToArray());
            var errorNode = tasks.FirstOrDefault(pair => !pair.Value.Result.IsSuccessStatusCode).Key;
            if (errorNode != null)
                throw new NodeException(errorNode);
            return tasks.ToDictionary(pair => pair.Key, pair => pair.Value.Result);
        }

        public static Dictionary<Node, T> Post<T>(string uri, Dictionary<Node, string> inputs)
        {
            Dictionary<Node, HttpResponseMessage> results = Post(uri, inputs);
            return results.ToDictionary(pair => pair.Key, pair =>
            {
                var readTask = pair.Value.Content.ReadAsStringAsync();
                readTask.Wait();
                return JsonConvert.DeserializeObject<T>(readTask.Result);
            });
        }

        public static List<Node> LoadFromPath(string path)
        {
            try
            {
                return LoadFromJson(File.ReadAllText(path));
            }
            catch (FileNotFoundException)
            {
                throw new MissingManifestException();
            }
        }

        public static List<Node> LoadFromJson(string json)
        {
            try
            {
                List<Node> nodes = JsonConvert.DeserializeObject<List<Node>>(json);
                Validate(nodes);
                return nodes;
            }
            catch (JsonException)
            {
                throw new MalformedManifestException();
            }
        }

        public static void Validate(List<Node> nodes)
        {
            if (nodes.Any(node => !node.Validate()))
            {
                throw new MalformedManifestException();
            }
        }
    }
}
