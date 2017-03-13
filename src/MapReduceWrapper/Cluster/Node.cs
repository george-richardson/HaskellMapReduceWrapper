using System;
using System.Net;
using System.Net.Http;

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global

namespace MapReduceWrapper.Cluster
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class Node
    {
        internal HttpClient Client { get; private set; }

        public string IP { get; set; }
        public ushort Port { get; set; } = 80;

        internal bool Validate()
        {
            try
            {
                IPAddress.Parse(IP);
                string uri = $"http://{IP}:{Port}/";
                HttpClient client = new HttpClient
                {
                    BaseAddress = new Uri(uri)
                };
                Client = client;
                return true;
            }
            catch
            {
                return false;
            }
        }

        public override string ToString()
        {
            return $"{IP}:{Port}";
        }
    }
}
