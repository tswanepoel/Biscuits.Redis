using Biscuits.Redis.Resp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
        
        #region Connection

        public async Task<string> SelectAsync(int index)
        {
            ValidateNotDisposed();

            if (index < 0 || index > 15)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "SELECT");
                    await WriteParameterBulkStringAsync(writer, index.ToString(CultureInfo.InvariantCulture));
                    await WriteEndCommandAsync(writer);
                }
                
                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadSimpleStringValue(reader);
                }
            }
        }

        public async Task<string> EchoAsync(string message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(await EchoAsync(bytes));
        }
        
        public async Task<byte[]> EchoAsync(byte[] message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "ECHO");
                    await WriteParameterBulkStringAsync(writer, message);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadBulkStringValue(reader);
                }
            }
        }

        #endregion
        
        #region Pub/Sub

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

        #endregion

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

        private static async Task WriteStartCommandAsync(RespWriter writer, string command)
        {
            await writer.WriteStartArrayAsync();
            await writer.WriteBulkStringAsync(command);
        }

        private static async Task WriteParameterSimpleStringUnsafeAsync(RespWriter writer, string value)
        {
            await writer.WriteSimpleStringUnsafeAsync(value);
        }

        private static async Task WriteParameterIntegerAsync(RespWriter writer, long value)
        {
            await writer.WriteIntegerAsync(value);
        }

        private static async Task WriteParameterBulkStringAsync(RespWriter writer, string value)
        {
            await writer.WriteBulkStringAsync(value);
        }

        private static async Task WriteParameterBulkStringAsync(RespWriter writer, byte[] value)
        {
            await writer.WriteBulkStringAsync(value);
        }

        private static async Task WriteEndCommandAsync(RespWriter writer)
        {
            await writer.WriteEndArrayAsync();
        }

        private static IList<byte[]> ReadArrayOfBulkStrings(RespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.Array)
            {
                throw new InvalidDataException();
            }
            
            if (!reader.TryReadStartArray(out long length))
            {
                return null;
            }

            var values = new List<byte[]>();

            for (int i = 0; i < length; i++)
            {
                dataType = reader.ReadDataType();

                if (dataType != RespDataType.BulkString)
                {
                    throw new InvalidDataException();
                }

                byte[] value = reader.ReadBulkStringValue();
                values.Add(value);
            }

            return values;
        }

        private static string ReadSimpleStringValue(RespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.SimpleString)
            {
                throw new InvalidDataException();
            }

            return reader.ReadSimpleStringValue();
        }

        private static byte[] ReadBulkStringValue(RespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.BulkString)
            {
                throw new InvalidDataException();
            }

            return reader.ReadBulkStringValue();
        }

        private static long ReadIntegerValue(RespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.Integer)
            {
                throw new InvalidDataException();
            }

            return reader.ReadIntegerValue();
        }
    }
}
