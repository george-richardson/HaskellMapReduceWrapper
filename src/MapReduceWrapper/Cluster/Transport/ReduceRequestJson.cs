using System.Collections.Generic;
using MapReduceWrapper.Manifest;

namespace MapReduceWrapper.Cluster.Transport
{
    public class ReduceRequestJson
    {
        public List<string> Keys { get; set; }
        public List<Node> Nodes { get; set; }
    }
}
