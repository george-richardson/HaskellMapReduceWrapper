using System.Collections.Generic;

namespace MapReduceWrapper.Cluster.Transport
{
    public class ReduceRequestJson
    {
        public List<string> Keys { get; set; }
        public List<string> Nodes { get; set; }
    }
}
