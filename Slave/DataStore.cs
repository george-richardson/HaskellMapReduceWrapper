using System.Collections.Generic;
using System.Linq;
using MapReduceWrapper.Cluster.Transport;

namespace MapReduceWrapper.Slave
{
    public static class DataStore
    {
        private static Dictionary<dynamic, List<dynamic>> _data;

        public static void SetData(Dictionary<dynamic, List<dynamic>> data)
        {
            _data = data;
        }

        public static Dictionary<dynamic, List<dynamic>> GetData(List<dynamic> keys)
        {
            return _data.Where(pair => keys.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        public static MapResponseJson GetKeyCounts()
        {

            return new MapResponseJson
            {
                Keys =
                    _data.Select(pair => new MapResponseJsonItem() {Count = pair.Value.Count, Key = pair.Key}).ToList()
            };
        }

        public static int Count()
        {
            return _data.Count;
        }
    }
}
