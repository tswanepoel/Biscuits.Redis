using System;
using System.IO;
using System.Net.Sockets;

namespace Biscuits.Redis
{
    public class RedisConnection : IDisposable
    {
        private readonly TcpClient _client;
        private bool _disposed;

        public RedisConnection(string hostname)
            : this(new RedisConnectionSettings(hostname))
        {
        }

        public RedisConnection(string hostname, int port)
            : this(new RedisConnectionSettings(hostname, port))
        {
        }

        public RedisConnection(RedisConnectionSettings settings)
        {
            // todo: allocate from pool
            _client = new TcpClient(settings.Hostname, settings.Port);
        }

        public Stream GetStream()
        {
            return _client.GetStream();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // todo: deallocate to pool
                _client.Dispose();
            }

            _disposed = true;
        }
    }
}
