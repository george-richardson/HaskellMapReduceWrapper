using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using MapReduceWrapper.Cluster.Exceptions;

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
                    WaitForSuccess(GetClient(address).PostAsync("load", new StreamContent(stream)), address);
                    stream.Seek(0, SeekOrigin.Begin);
                }
            }
        }

        public void ExecuteProgram()
        {
            foreach (IPAddress address in _manifest)
            {
                WaitForSuccess(GetClient(address).PostAsync("run", new StringContent("")), address);
            }
        }

        private bool Ping(IPAddress address)
        {
            bool result = false;
            try
            {
                var response = GetClient(address).GetAsync("ping");
                response.Wait();
                result = response.Result.IsSuccessStatusCode;
            }
            catch
            {
                //ignore
            }

            return result;
        }

        private HttpClient GetClient(IPAddress address)
        {
            string uri = $"http://{address}:80/";
            HttpClient client = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(3),
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
