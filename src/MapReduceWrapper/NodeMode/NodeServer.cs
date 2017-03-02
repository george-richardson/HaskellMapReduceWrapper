using System.IO;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace MapReduceWrapper.NodeMode
{
    public class NodeServer
    {
        private NodeServer()
        {
            var config = new ConfigurationBuilder().Build();

            var builder = new WebHostBuilder()
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseConfiguration(config)
                .UseStartup<Startup>()
                .UseKestrel()
                .UseUrls("http://0.0.0.0:80");

            var host = builder.Build();
            host.Run();
        }

        public static void Start()
        {
            // ReSharper disable once UnusedVariable
            var nodeServer = new NodeServer();
        }
    }
}
