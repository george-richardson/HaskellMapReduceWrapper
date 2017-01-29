using System;
using System.Net;

namespace MapReduceWrapper.Cluster.Exceptions
{
    public class NodeException : Exception
    {
        public NodeException(IPAddress address) : base($"Node at {address} has encountered an error")
        {
            
        }
    }
}
