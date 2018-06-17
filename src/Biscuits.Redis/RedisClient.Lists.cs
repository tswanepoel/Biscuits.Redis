using Biscuits.Redis.Commands;
using Biscuits.Redis.Resp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region LIndex

        public async Task<string> LIndexAsync(string key, long index)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LIndex(connection.GetStream(), key, index);
                return (await command.ExecuteAsync()).Result;
            }
        }

        #endregion

        public async Task<long> LInsertBeforeAsync(string key, string before, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LINSERT");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, "BEFORE");
                    await WriteParameterBulkStringAsync(writer, before);
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<long> LInsertAfterAsync(string key, string after, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LINSERT");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, "AFTER");
                    await WriteParameterBulkStringAsync(writer, after);
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<long> LLenAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LLenAsync(keyBytes);
        }

        public async Task<long> LLenAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LLEN");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<string> LPopAsync(string key)
        {
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LPOP");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadBulkStringValue(reader);
                }
            }
        }

        public async Task<long> LPushAsync(string key, params string[] values)
        {
            ValidateNotDisposed();

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

        public async Task<long> LPushAsync(byte[] key, IList<byte[]> values)
        {
            ValidateNotDisposed();

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

            if (values.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(values));
            }

            for (int i = 0; i < values.Count; i++)
            {
                if (values[i] == null)
                {
                    throw new ArgumentException(nameof(values));
                }
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LPUSH");
                    await WriteParameterBulkStringAsync(writer, key);

                    for (int i = 0; i < values.Count; i++)
                    {
                        await WriteParameterBulkStringAsync(writer, values[i]);
                    }

                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<long> LPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LPUSHX");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<IList<string>> LRangeAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LRANGE");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, start.ToString(CultureInfo.InvariantCulture));
                    await WriteParameterBulkStringAsync(writer, stop.ToString(CultureInfo.InvariantCulture));
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadArrayOfBulkStrings(reader);
                }
            }
        }

        public async Task<long> LRemAsync(string key, long count, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LREM");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, count.ToString(CultureInfo.InvariantCulture));
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<string> LSetAsync(string key, long index, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LSET");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, index.ToString(CultureInfo.InvariantCulture));
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadSimpleStringValue(reader);
                }
            }
        }

        public async Task<string> LTrimAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "LTRIM");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, start.ToString(CultureInfo.InvariantCulture));
                    await WriteParameterBulkStringAsync(writer, stop.ToString(CultureInfo.InvariantCulture));
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadSimpleStringValue(reader);
                }
            }
        }

        public async Task<string> RPopAsync(string key)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.Length == 0)
            {
                throw new ArgumentException(nameof(key));
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "RPOP");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadBulkStringValue(reader);
                }
            }
        }

        public async Task<long> RPushAsync(string key, params string[] values)
        {
            ValidateNotDisposed();

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

        public async Task<long> RPushAsync(byte[] key, IList<byte[]> values)
        {
            ValidateNotDisposed();

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

            if (values.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(values));
            }

            for (int i = 0; i < values.Count; i++)
            {
                if (values[i] == null)
                {
                    throw new ArgumentException(nameof(values));
                }
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "RPUSH");
                    await WriteParameterBulkStringAsync(writer, key);

                    for (int i = 0; i < values.Count; i++)
                    {
                        await WriteParameterBulkStringAsync(writer, values[i]);
                    }

                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<long> RPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "RPUSHX");
                    await WriteParameterBulkStringAsync(writer, key);
                    await WriteParameterBulkStringAsync(writer, value);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadIntegerValue(reader);
                }
            }
        }

        public async Task<string> RPopLPushAsync(string source, string destination)
        {
            ValidateNotDisposed();

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
            ValidateNotDisposed();

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

            using (var connection = new RedisConnection(_connectionSettings))
            {
                using (var writer = new RespWriter(connection.GetStream()))
                {
                    await WriteStartCommandAsync(writer, "RPOPLPUSH");
                    await WriteParameterBulkStringAsync(writer, source);
                    await WriteParameterBulkStringAsync(writer, destination);
                    await WriteEndCommandAsync(writer);
                }

                using (var reader = new RespReader(connection.GetStream()))
                {
                    return ReadBulkStringValue(reader);
                }
            }
        }
    }
}
