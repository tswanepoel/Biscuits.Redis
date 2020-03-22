namespace Biscuits.Redis
{
    public class RedisConnectionSettings
    {
        public RedisConnectionSettings(string hostname)
            : this(hostname, 6379)
        {
        }

        public RedisConnectionSettings(string hostname, int port)
        {
            Hostname = hostname;
            Port = port;
        }

        public string Hostname { get; set; }

        public int Port { get; set; }
    }
}
