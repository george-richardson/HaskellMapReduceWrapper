using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MapReduceWrapper.Cluster
{
    public class FileSplitter
    {
        private List<string> _fileContent;
        private int _splits;
        private readonly int _itemsPerSplit;

        public FileSplitter(string path, int splits)
        {
            _fileContent = File.ReadLines(path).ToList();
            _splits = splits;
            _itemsPerSplit = _fileContent.Count/_splits;
        }

        public IEnumerable<string> TakeOne()
        {
            switch (_splits)
            {
                case 0:
                    throw new Exception("Empty splitter");
                case 1:
                    _splits--;
                    return _fileContent;
                default:
                    _splits--;
                    IEnumerable<string> result = _fileContent.Take(_itemsPerSplit);
                    _fileContent = _fileContent.GetRange(_itemsPerSplit, _fileContent.Count - _itemsPerSplit);
                    return result;
            }
        }
    }
}
