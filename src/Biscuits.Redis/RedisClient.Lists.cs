using Biscuits.Redis.Commands.Lists;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region LIndex

        public string LIndex(string key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = LIndex(keyBytes, index);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public byte[] LIndex(byte[] key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LIndex(connection.GetStream(), key, index);
                return command.Execute();
            }
        }

        public async Task<string> LIndexAsync(string key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await LIndexAsync(keyBytes, index);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> LIndexAsync(byte[] key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LIndex(connection.GetStream(), key, index);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LInsertBefore

        public long LInsertBefore(string key, string before, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (before == null)
                throw new ArgumentNullException(nameof(before));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] beforeBytes = _encoding.GetBytes(before);
            byte[] valueBytes = _encoding.GetBytes(value);

            return LInsertBefore(keyBytes, beforeBytes, valueBytes);
        }

        public long LInsertBefore(byte[] key, byte[] before, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (before == null)
                throw new ArgumentNullException(nameof(before));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LInsertBefore(connection.GetStream(), key, before, value);
                return command.Execute();
            }
        }

        public async Task<long> LInsertBeforeAsync(string key, string before, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (before == null)
                throw new ArgumentNullException(nameof(before));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] beforeBytes = _encoding.GetBytes(before);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LInsertBeforeAsync(keyBytes, beforeBytes, valueBytes);
        }

        public async Task<long> LInsertBeforeAsync(byte[] key, byte[] before, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (before == null)
                throw new ArgumentNullException(nameof(before));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LInsertBefore(connection.GetStream(), key, before, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LInsertAfter

        public long LInsertAfter(string key, string after, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (after == null)
                throw new ArgumentNullException(nameof(after));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] afterBytes = _encoding.GetBytes(after);
            byte[] valueBytes = _encoding.GetBytes(value);

            return LInsertAfter(keyBytes, afterBytes, valueBytes);
        }

        public long LInsertAfter(byte[] key, byte[] after, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (after == null)
                throw new ArgumentNullException(nameof(after));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LInsertAfter(connection.GetStream(), key, after, value);
                return command.Execute();
            }
        }

        public async Task<long> LInsertAfterAsync(string key, string after, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (after == null)
                throw new ArgumentNullException(nameof(after));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] afterBytes = _encoding.GetBytes(after);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LInsertAfterAsync(keyBytes, afterBytes, valueBytes);
        }

        public async Task<long> LInsertAfterAsync(byte[] key, byte[] after, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (after == null)
                throw new ArgumentNullException(nameof(after));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LInsertAfter(connection.GetStream(), key, after, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LLen

        public long LLen(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return LLen(keyBytes);
        }

        public long LLen(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LLen(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<long> LLenAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LLenAsync(keyBytes);
        }

        public async Task<long> LLenAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LLen(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LPop

        public string LPop(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = LPop(keyBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public byte[] LPop(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPop(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<string> LPopAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await LPopAsync(keyBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> LPopAsync(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPop(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LPush

        public long LPush(string key, params string[] values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var valuesBytes = new List<byte[]>(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                    throw new ArgumentException(nameof(values));

                valuesBytes.Add(_encoding.GetBytes(values[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return LPush(keyBytes, valuesBytes);
        }

        public long LPush(byte[] key, ICollection<byte[]> values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (values.Count == 0)
                throw new ArgumentException(nameof(values));

            if (values.Any(x => x == null))
                throw new ArgumentException(nameof(values));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPush(connection.GetStream(), key, values);
                return command.Execute();
            }
        }

        public async Task<long> LPushAsync(string key, params string[] values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var valuesBytes = new List<byte[]>(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                    throw new ArgumentException(nameof(values));

                valuesBytes.Add(_encoding.GetBytes(values[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LPushAsync(keyBytes, valuesBytes);
        }

        public async Task<long> LPushAsync(byte[] key, ICollection<byte[]> values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (values.Count == 0)
                throw new ArgumentException(nameof(values));

            if (values.Any(x => x == null))
                throw new ArgumentException(nameof(values));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPush(connection.GetStream(), key, values);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LPushX

        public long LPushX(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return LPushX(keyBytes, valueBytes);
        }

        public long LPushX(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPushX(connection.GetStream(), key, value);
                return command.Execute();
            }
        }

        public async Task<long> LPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LPushXAsync(keyBytes, valueBytes);
        }

        public async Task<long> LPushXAsync(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LPushX(connection.GetStream(), key, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LRange

        public IList<string> LRange(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes = LRange(keyBytes, start, stop);

            var values = new List<string>(valuesBytes.Count);

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                string value = _encoding.GetString(valuesBytes[i]);
                values.Add(value);
            }

            return values;
        }

        public IList<byte[]> LRange(byte[] key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LRange(connection.GetStream(), key, start, stop);
                return command.Execute();
            }
        }

        public async Task<IList<string>> LRangeAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

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
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LRange(connection.GetStream(), key, start, stop);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LRem

        public long LRem(string key, long count, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return LRem(keyBytes, count, valueBytes);
        }

        public long LRem(byte[] key, long count, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LRem(connection.GetStream(), key, count, value);
                return command.Execute();
            }
        }

        public async Task<long> LRemAsync(string key, long count, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LRemAsync(keyBytes, count, valueBytes);
        }

        public async Task<long> LRemAsync(byte[] key, long count, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LRem(connection.GetStream(), key, count, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LSet

        public string LSet(string key, long index, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return LSet(keyBytes, index, valueBytes);
        }

        public string LSet(byte[] key, long index, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LSet(connection.GetStream(), key, index, value);
                return command.Execute();
            }
        }

        public async Task<string> LSetAsync(string key, long index, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await LSetAsync(keyBytes, index, valueBytes);
        }

        public async Task<string> LSetAsync(byte[] key, long index, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LSet(connection.GetStream(), key, index, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region LTrim

        public string LTrim(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return LTrim(keyBytes, start, stop);
        }

        public string LTrim(byte[] key, long start, long stop)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LTrim(connection.GetStream(), key, start, stop);
                return command.Execute();
            }
        }

        public async Task<string> LTrimAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return await LTrimAsync(keyBytes, start, stop);
        }

        public async Task<string> LTrimAsync(byte[] key, long start, long stop)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new LTrim(connection.GetStream(), key, start, stop);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region RPop

        public string RPop(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = RPop(keyBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public byte[] RPop(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPop(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<string> RPopAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = await RPopAsync(keyBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> RPopAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPop(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region RPush

        public long RPush(string key, params string[] values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var valuesBytes = new List<byte[]>(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                    throw new ArgumentException(nameof(values));

                valuesBytes.Add(_encoding.GetBytes(values[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return RPush(keyBytes, valuesBytes);
        }

        public long RPush(byte[] key, IList<byte[]> values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (values.Count == 0)
                throw new ArgumentException(nameof(values));

            if (values.Any(x => x == null))
                throw new ArgumentException(nameof(values));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPush(connection.GetStream(), key, values);
                return command.Execute();
            }
        }

        public async Task<long> RPushAsync(string key, params string[] values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var valuesBytes = new List<byte[]>(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                    throw new ArgumentException(nameof(values));

                valuesBytes.Add(_encoding.GetBytes(values[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await RPushAsync(keyBytes, valuesBytes);
        }

        public async Task<long> RPushAsync(byte[] key, IList<byte[]> values)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (values.Count == 0)
                throw new ArgumentException(nameof(values));

            if (values.Any(x => x == null))
                throw new ArgumentException(nameof(values));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPush(connection.GetStream(), key, values);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region RPushX

        public long RPushX(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return RPushX(keyBytes, valueBytes);
        }

        public long RPushX(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPushX(connection.GetStream(), key, value);
                return command.Execute();
            }
        }

        public async Task<long> RPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] valueBytes = _encoding.GetBytes(value);

            return await RPushXAsync(keyBytes, valueBytes);
        }

        public async Task<long> RPushXAsync(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPushX(connection.GetStream(), key, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region RPopLPush

        public string RPopLPush(string source, string destination)
        {
            ValidateNotDisposed();

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            byte[] sourceBytes = _encoding.GetBytes(source);
            byte[] destinationBytes = _encoding.GetBytes(destination);
            byte[] valueBytes = RPopLPush(sourceBytes, destinationBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public byte[] RPopLPush(byte[] source, byte[] destination)
        {
            ValidateNotDisposed();

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (source.Length == 0)
                throw new ArgumentException(nameof(source));

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            if (destination.Length == 0)
                throw new ArgumentException(nameof(destination));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPopLPush(connection.GetStream(), source, destination);
                return command.Execute();
            }
        }

        public async Task<string> RPopLPushAsync(string source, string destination)
        {
            ValidateNotDisposed();

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            byte[] sourceBytes = _encoding.GetBytes(source);
            byte[] destinationBytes = _encoding.GetBytes(destination);
            byte[] valueBytes = await RPopLPushAsync(sourceBytes, destinationBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> RPopLPushAsync(byte[] source, byte[] destination)
        {
            ValidateNotDisposed();

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (source.Length == 0)
                throw new ArgumentException(nameof(source));

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            if (destination.Length == 0)
                throw new ArgumentException(nameof(destination));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new RPopLPush(connection.GetStream(), source, destination);
                return await command.ExecuteAsync();
            }
        }

        #endregion
    }
}
