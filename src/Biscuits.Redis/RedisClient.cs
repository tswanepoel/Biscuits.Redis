using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient : IDisposable
    {
        private readonly RedisConnectionSettings _connectionSettings;
        private readonly Encoding _encoding;

        public RedisClient(string hostname)
            : this(new RedisConnectionSettings(hostname))
        {
        }

        public RedisClient(string hostname, int port)
            : this(new RedisConnectionSettings(hostname, port))
        {
        }

        public RedisClient(string hostname, int port, Encoding encoding)
            : this(new RedisConnectionSettings(hostname, port), encoding)
        {
        }

        public RedisClient(RedisConnectionSettings connectionSettings)
            : this(connectionSettings, Encoding.UTF8)
        {
        }

        public RedisClient(RedisConnectionSettings connectionSettings, Encoding encoding)
        {
            _connectionSettings = connectionSettings ?? throw new ArgumentNullException(nameof(connectionSettings));
            _encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
        }
        
        public async Task<RedisSubscription> SubscribeAsync(params string[] channels)
        {
            var subscription = new RedisSubscription(_connectionSettings, _encoding, channels);
            await subscription.SubscribeAsync();

            return subscription;
        }

        public async Task<RedisSubscription> SubscribeAsync(IList<byte[]> channels)
        {
            var subscription = new RedisSubscription(_connectionSettings, _encoding, channels);
            await subscription.SubscribeAsync();

            return subscription;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        private void ValidateNotDisposed()
        {
        }
    }
}
