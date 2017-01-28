using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using MapReduceWrapper.Manifest;
using Newtonsoft.Json;
using MapReduceWrapper.Manifest.Exceptions;

namespace MapReduceWrapper
{
    public class NodeManifest : IEnumerable<IPAddress>
    {
        public static NodeManifest Load(string path)
        {
            string manifestText;
            NodeManifestJson manifestJson;
            NodeManifest manifest;
            try
            {
                manifestText = File.ReadAllText(path);
            } catch
            {
                throw new MissingManifestException();
            }
            try
            {
                manifestJson = JsonConvert.DeserializeObject<NodeManifestJson>(manifestText);
                manifest = new NodeManifest(manifestJson);
            } catch
            {
                throw new MalformedManifestException();
            }
            return manifest;
        }

        private readonly List<IPAddress> _ipAddresses;

        private NodeManifest(NodeManifestJson json)
        {
            _ipAddresses = json.Nodes.Select(IPAddress.Parse).ToList();
        }

        public IEnumerator<IPAddress> GetEnumerator()
        {
            return _ipAddresses.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
