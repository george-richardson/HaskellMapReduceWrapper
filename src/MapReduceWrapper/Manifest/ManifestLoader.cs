using System.Collections.Generic;
using System.IO;
using System.Linq;
using MapReduceWrapper.Manifest.Exceptions;
using Newtonsoft.Json;

namespace MapReduceWrapper.Manifest
{
    public static class ManifestLoader
    {
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
