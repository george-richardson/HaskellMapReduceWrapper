using System;
using MapReduceWrapper.Manifest;

namespace MapReduceWrapper.Cluster.Exceptions
{
    public class NodeException : Exception
    {
        public NodeException(Node node) : base($"Node at {node.IP}:{node.Port} has encountered an error")
        {
            
        }
    }
}
