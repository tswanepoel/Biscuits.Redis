using Biscuits.Redis.Resp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public class RedisClient : IDisposable
    {
        private readonly TcpClient _client;
        private readonly Encoding _encoding;
        private RespWriter _writer;
        private RespReader _reader;
        private bool _initialized;
        private bool _disposed;

        public RedisClient(string hostname)
            : this(hostname, 6379)
        {
        }

        public RedisClient(string hostname, int port)
             : this(hostname, port, Encoding.UTF8)
        {
        }

        public RedisClient(string hostname, int port, Encoding encoding)
        {
            _client = new TcpClient(hostname, port);
            _encoding = encoding;
        }

        public void Initialize()
        {
            if (!_initialized)
            {
                _writer = new RespWriter(_client.GetStream());
                _reader = new RespReader(_client.GetStream());

                _initialized = true;
            }
        }

        #region Connection

        public async Task<string> SelectAsync(int index)
        {
            if (index < 0 || index > 15)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            Initialize();

            await WriteStartCommandAsync("SELECT");
            await WriteParameterBulkStringAsync(index.ToString(CultureInfo.InvariantCulture));
            await WriteEndCommandAsync();

            return ReadSimpleStringValue();
        }

        public async Task<string> EchoAsync(string message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(await EchoAsync(bytes));
        }
        
        public async Task<byte[]> EchoAsync(byte[] message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            Initialize();

            await WriteStartCommandAsync("ECHO");
            await WriteParameterBulkStringAsync(message);
            await WriteEndCommandAsync();

            return ReadBulkStringValue();
        }

        #endregion

        #region Lists

        public async Task<string> LIndexAsync(string key, long index)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await LIndexAsync(keyBytes, index);

            if (valueBytes == null)
            {
                return null;
            }

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> LIndexAsync(byte[] key, long index)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }
            
            Initialize();

            await WriteStartCommandAsync("LINDEX");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(index.ToString(CultureInfo.InvariantCulture));
            await WriteEndCommandAsync();

            return ReadBulkStringValue();
        }

        public async Task<long> LInsertBeforeAsync(string key, string before, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (before == null)
            {
                throw new ArgumentNullException(nameof(before));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] beforeBytes = _encoding.GetBytes(before);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LInsertBeforeAsync(keyBytes, beforeBytes, valueBytes);
        }

        public async Task<long> LInsertBeforeAsync(byte[] key, byte[] before, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (before == null)
            {
                throw new ArgumentNullException(nameof(before));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Initialize();

            await WriteStartCommandAsync("LINSERT");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync("BEFORE");
            await WriteParameterBulkStringAsync(before);
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<long> LInsertAfterAsync(string key, string after, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (after == null)
            {
                throw new ArgumentNullException(nameof(after));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] afterBytes = _encoding.GetBytes(after);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LInsertAfterAsync(keyBytes, afterBytes, valueBytes);
        }

        public async Task<long> LInsertAfterAsync(byte[] key, byte[] after, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (after == null)
            {
                throw new ArgumentNullException(nameof(after));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Initialize();

            await WriteStartCommandAsync("LINSERT");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync("AFTER");
            await WriteParameterBulkStringAsync(after);
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<long> LLenAsync(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LLenAsync(keyBytes);
        }

        public async Task<long> LLenAsync(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            Initialize();

            await WriteStartCommandAsync("LLEN");
            await WriteParameterBulkStringAsync(key);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<string> LPopAsync(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await LPopAsync(keyBytes);

            if (valueBytes == null)
            {
                return null;
            }

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> LPopAsync(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            Initialize();

            await WriteStartCommandAsync("LPOP");
            await WriteParameterBulkStringAsync(key);
            await WriteEndCommandAsync();

            return ReadBulkStringValue();
        }

        public async Task<long> LPushAsync(string key, params string[] values)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            var valuesBytes = new byte[values.Length][];

            for (int i = 0; i < values.Length; i++)
            {
                valuesBytes[i] = values[i] != null ? _encoding.GetBytes(values[i]) : null;
            }

            return await LPushAsync(keyBytes, valuesBytes);
        }

        public async Task<long> LPushAsync(byte[] key, params byte[][] values)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            if (values.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(values));
            }

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                {
                    throw new ArgumentException(nameof(values));
                }
            }

            Initialize();

            await WriteStartCommandAsync("LPUSH");
            await WriteParameterBulkStringAsync(key);

            for (int i = 0; i < values.Length; i++)
            {
                await WriteParameterBulkStringAsync(values[i]);
            }

            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<long> LPushXAsync(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);
            
            return await LPushXAsync(keyBytes, valueBytes);
        }

        public async Task<long> LPushXAsync(byte[] key, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            
            Initialize();

            await WriteStartCommandAsync("LPUSHX");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<IList<string>> LRangeAsync(string key, long start, long stop)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes = await LRangeAsync(keyBytes, start, stop);

            var values = new List<string>(valuesBytes.Count);

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                string value = _encoding.GetString(valuesBytes[i]);
                values.Add(value);
            }

            return values;
        }

        public async Task<IList<byte[]>> LRangeAsync(byte[] key, long start, long stop)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            Initialize();

            await WriteStartCommandAsync("LRANGE");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(start.ToString(CultureInfo.InvariantCulture));
            await WriteParameterBulkStringAsync(stop.ToString(CultureInfo.InvariantCulture));
            await WriteEndCommandAsync();

            return ReadArrayOfBulkStrings();
        }

        public async Task<long> LRemAsync(string key, long count, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LRemAsync(keyBytes, count, valueBytes);
        }

        public async Task<long> LRemAsync(byte[] key, long count, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Initialize();

            await WriteStartCommandAsync("LREM");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(count.ToString(CultureInfo.InvariantCulture));
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<string> LSetAsync(string key, long index, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LSetAsync(keyBytes, index, valueBytes);
        }

        public async Task<string> LSetAsync(byte[] key, long index, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Initialize();

            await WriteStartCommandAsync("LSET");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(index.ToString(CultureInfo.InvariantCulture));
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadSimpleStringValue();
        }

        public async Task<string> LTrimAsync(string key, long start, long stop)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LTrimAsync(keyBytes, start, stop);
        }

        public async Task<string> LTrimAsync(byte[] key, long start, long stop)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            Initialize();

            await WriteStartCommandAsync("LTRIM");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(start.ToString(CultureInfo.InvariantCulture));
            await WriteParameterBulkStringAsync(stop.ToString(CultureInfo.InvariantCulture));
            await WriteEndCommandAsync();

            return ReadSimpleStringValue();
        }

        public async Task<string> RPopAsync(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await RPopAsync(keyBytes);

            if (valueBytes == null)
            {
                return null;
            }

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> RPopAsync(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            Initialize();

            await WriteStartCommandAsync("RPOP");
            await WriteParameterBulkStringAsync(key);
            await WriteEndCommandAsync();

            return ReadBulkStringValue();
        }

        public async Task<long> RPushAsync(string key, params string[] values)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            var valuesBytes = new byte[values.Length][];

            for (int i = 0; i < values.Length; i++)
            {
                valuesBytes[i] = values[i] != null ? _encoding.GetBytes(values[i]) : null;
            }

            return await RPushAsync(keyBytes, valuesBytes);
        }
        
        public async Task<long> RPushAsync(byte[] key, params byte[][] values)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            if (values.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(values));
            }

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                {
                    throw new ArgumentException(nameof(values));
                }
            }

            Initialize();

            await WriteStartCommandAsync("RPUSH");
            await WriteParameterBulkStringAsync(key);

            for (int i = 0; i < values.Length; i++)
            {
                await WriteParameterBulkStringAsync(values[i]);
            }

            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<long> RPushXAsync(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await RPushXAsync(keyBytes, valueBytes);
        }

        public async Task<long> RPushXAsync(byte[] key, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Initialize();

            await WriteStartCommandAsync("RPUSHX");
            await WriteParameterBulkStringAsync(key);
            await WriteParameterBulkStringAsync(value);
            await WriteEndCommandAsync();

            return ReadIntegerValue();
        }

        public async Task<string> RPopLPushAsync(string source, string destination)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            byte[] sourceBytes = _encoding.GetBytes(source);
            byte[] destinationBytes = _encoding.GetBytes(destination);

            byte[] valueBytes = await RPopLPushAsync(sourceBytes, destinationBytes);

            if (valueBytes == null)
            {
                return null;
            }

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> RPopLPushAsync(byte[] source, byte[] destination)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (source.Length == 0)
            {
                throw new ArgumentException(nameof(source));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            if (destination.Length == 0)
            {
                throw new ArgumentException(nameof(destination));
            }

            Initialize();

            await WriteStartCommandAsync("RPOPLPUSH");
            await WriteParameterBulkStringAsync(source);
            await WriteParameterBulkStringAsync(destination);
            await WriteEndCommandAsync();

            return ReadBulkStringValue();
        }

        #endregion

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected async Task WriteStartCommandAsync(string command)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteStartArrayAsync();
            await _writer.WriteBulkStringAsync(command);
        }

        protected async Task WriteParameterSimpleStringUnsafeAsync(string value)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteSimpleStringUnsafeAsync(value);
        }

        protected async Task WriteParameterIntegerAsync(long value)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteIntegerAsync(value);
        }

        protected async Task WriteParameterBulkStringAsync(string value)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteBulkStringAsync(value);
        }

        protected async Task WriteParameterBulkStringAsync(byte[] value)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteBulkStringAsync(value);
        }

        protected async Task WriteEndCommandAsync()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            await _writer.WriteEndArrayAsync();
        }

        protected IList<byte[]> ReadArrayOfBulkStrings()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

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

            long length;

            if (!_reader.TryReadStartArray(out length))
            {
                return null;
            }

            var values = new List<byte[]>();

            for (int i = 0; i < length; i++)
            {
                dataType =  _reader.ReadDataType();

                if (dataType != RespDataType.BulkString)
                {
                    throw new InvalidDataException();
                }

                byte[] value = _reader.ReadBulkStringValue();
                values.Add(value);
            }

            return values;
        }

        protected string ReadSimpleStringValue()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            RespDataType dataType = _reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = _reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.SimpleString)
            {
                throw new InvalidDataException();
            }

            return _reader.ReadSimpleStringValue();
        }

        protected byte[] ReadBulkStringValue()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            RespDataType dataType = _reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = _reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.BulkString)
            {
                throw new InvalidDataException();
            }

            return _reader.ReadBulkStringValue();
        }

        protected long ReadIntegerValue()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RedisClient));
            }

            RespDataType dataType = _reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = _reader.ReadErrorValue();
                throw new RedisErrorException(err);
            }

            if (dataType != RespDataType.Integer)
            {
                throw new InvalidDataException();
            }

            return _reader.ReadIntegerValue();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _client.Dispose();
            }

            _disposed = true;
        }
    }
}
