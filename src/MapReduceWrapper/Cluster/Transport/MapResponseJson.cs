﻿using System.Collections.Generic;

namespace MapReduceWrapper.Cluster.Transport
{
    public class MapResponseJson
    {
        public List<MapResponseJsonItem> Keys { get; set; }
    }
    public class MapResponseJsonItem
    {
        public string Key { get; set; }
        public int Count { get; set; }
    }
}
