using System;
using System.Linq;
using System.Net;
using System.Net.Http;

namespace MapReduceWrapper.Cluster
{
    public class Cluster
    {
        private readonly NodeManifest _manifest;

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

        public void LoadProgram()
        {
            //todo implement uploading program to cluster
        }

        public void ExecuteProgram()
        {
            //todo implement executing program on cluster
        }

        private bool Ping(IPAddress address)
        {
            bool result = false;
            try
            {
                string uri = $"http://{address}:80/";
                HttpClient client = new HttpClient
                {
                    Timeout = TimeSpan.FromSeconds(3),
                    BaseAddress = new Uri(uri)
                };
                var response = client.GetAsync("ping");
                response.Wait();
                result = response.Result.IsSuccessStatusCode;
            }
            catch
            {
                //ignore
            }

            return result;
        }
    }
}
