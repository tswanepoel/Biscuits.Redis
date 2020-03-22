using Biscuits.Redis.Resp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public class RedisSubscription : IDisposable
    {
        private readonly RedisConnection _connection;
        private readonly RespWriter _writer;
        private readonly RespReader _reader;
        private readonly Encoding _encoding;
        private readonly IList<byte[]> _channels;
        private CancellationTokenSource _source;
        private bool _subscribed;
        private bool _disposed;

        public RedisSubscription(string hostname, params string[] channels)
           : this(new RedisConnectionSettings(hostname), channels)
        {
        }

        public RedisSubscription(string hostname, int port, params string[] channels)
            : this(new RedisConnectionSettings(hostname, port), channels)
        {
        }

        public RedisSubscription(string hostname, int port, Encoding encoding, params string[] channels)
            : this(new RedisConnectionSettings(hostname, port), encoding, channels)
        {
        }

        public RedisSubscription(RedisConnectionSettings connectionSettings, params string[] channels)
            : this(connectionSettings, Encoding.UTF8, channels)
        {
        }
        
        public RedisSubscription(RedisConnectionSettings connectionSettings, Encoding encoding, params string[] channels)
            : this(connectionSettings, encoding)
        {
            if (channels == null)
            {
                throw new ArgumentNullException(nameof(channels));
            }

            var channelsBytes = new byte[channels.Length][];

            for (var i = 0; i < channels.Length; i++)
            {
                channelsBytes[i] = _encoding.GetBytes(channels[i]);
            }

            _channels = channelsBytes;
        }

        public RedisSubscription(RedisConnectionSettings connectionSettings, Encoding encoding, IList<byte[]> channels)
            : this(connectionSettings, encoding)
        {
            _channels = channels ?? throw new ArgumentNullException(nameof(channels));
        }

        private RedisSubscription(RedisConnectionSettings connectionSettings, Encoding encoding)
        {
            _connection = new RedisConnection(connectionSettings);
            _writer = new RespWriter(_connection.GetStream());
            _reader = new RespReader(_connection.GetStream());
            _encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
        }

        public async Task SubscribeAsync()
        {
            ValidateNotDisposed();

            if (!_subscribed)
            {
                await WriteStartCommandAsync(_writer, "SUBSCRIBE");

                for (var i = 0; i < _channels.Count; i++)
                {
                    await WriteParameterBulkStringAsync(_writer, _channels[i]);
                }

                await WriteEndCommandAsync(_writer);

                _source = new CancellationTokenSource();
                var task = Task.Run(() => Listen(), _source.Token);
                
                _subscribed = true;
            }
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
                _connection.Dispose();
                _writer.Dispose();
                _reader.Dispose();

                if (_subscribed)
                {
                    _source.Cancel();
                }
            }

            _disposed = true;
        }

        private void Listen()
        {
            while (true)
            {
                ValidateNotDisposed();
                RespDataType dataType = _reader.ReadDataType();

                if (dataType == RespDataType.Error)
                {
                    string err = _reader.ReadErrorValue();
                    throw new RedisErrorException(err);
                }

                if (dataType != RespDataType.Array)
                {
                    throw new InvalidDataException();
                }

                if (!_reader.TryReadStartArray(out _))
                {
                    continue;
                }

                dataType = _reader.ReadDataType();

                if (dataType != RespDataType.BulkString)
                {
                    throw new InvalidDataException();
                }

                byte[] kindBytes = _reader.ReadBulkStringValue();
                string kind = _encoding.GetString(kindBytes);

                switch (kind)
                {
                    case "subscribe":
                        {
                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.BulkString)
                            {
                                throw new InvalidDataException();
                            }

                            byte[] channelBytes = _reader.ReadBulkStringValue();
                            string channel = _encoding.GetString(channelBytes);

                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.Integer)
                            {
                                throw new InvalidDataException();
                            }

                            long count = _reader.ReadIntegerValue();
                            Debug.WriteLine("subscribe: {0} ({1})", channel, count);
                            break;
                        }

                    case "unsubscribe":
                        {
                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.BulkString)
                            {
                                throw new InvalidDataException();
                            }

                            byte[] channelBytes = _reader.ReadBulkStringValue();
                            string channel = _encoding.GetString(channelBytes);

                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.Integer)
                            {
                                throw new InvalidDataException();
                            }

                            long count = _reader.ReadIntegerValue();
                            Debug.WriteLine("unsubscribe: {0} ({1})", channel, count);
                            break;
                        }

                    case "message":
                        {
                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.BulkString)
                            {
                                throw new InvalidDataException();
                            }

                            byte[] channelBytes = _reader.ReadBulkStringValue();
                            string channel = _encoding.GetString(channelBytes);

                            dataType = _reader.ReadDataType();

                            if (dataType != RespDataType.BulkString)
                            {
                                throw new InvalidDataException();
                            }

                            byte[] messageBytes = _reader.ReadBulkStringValue();
                            string messageString = _encoding.GetString(messageBytes);

                            Debug.WriteLine("message from {0}: {1}", channel, messageString);
                            break;
                        }
                }
            }
        }

        private void ValidateNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisSubscription));
            }
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

            for (var i = 0; i < length; i++)
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
