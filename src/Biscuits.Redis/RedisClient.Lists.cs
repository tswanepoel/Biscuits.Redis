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
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = LIndexCore(_encoding.GetBytes(key), index);
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public byte[] LIndex(byte[] key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LIndexCore(key, index);
        }

        public async Task<string> LIndexAsync(string key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = await LIndexCoreAsync(_encoding.GetBytes(key), index);
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public async Task<byte[]> LIndexAsync(byte[] key, long index)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LIndexCoreAsync(key, index);
        }

        private byte[] LIndexCore(byte[] key, long index)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LIndex(connection.GetStream(), key, index);

            return command.Execute();
        }

        private async Task<byte[]> LIndexCoreAsync(byte[] key, long index)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LIndex(connection.GetStream(), key, index);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LInsertBefore

        public long LInsertBefore(string key, string before, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LInsertBeforeCore(
                _encoding.GetBytes(key),
                before != null ? _encoding.GetBytes(before) : null,
                value != null ? _encoding.GetBytes(value) : null);
        }

        public long LInsertBefore(byte[] key, byte[] before, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LInsertBeforeCore(key, before, value);
        }

        public async Task<long> LInsertBeforeAsync(string key, string before, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LInsertBeforeCoreAsync(
                _encoding.GetBytes(key), 
                before != null ? _encoding.GetBytes(before) : null, 
                value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> LInsertBeforeAsync(byte[] key, byte[] before, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LInsertBeforeCoreAsync(key, before, value);
        }

        private long LInsertBeforeCore(byte[] key, byte[] before, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LInsertBefore(connection.GetStream(), key, before, value);

            return command.Execute();
        }

        private async Task<long> LInsertBeforeCoreAsync(byte[] key, byte[] before, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LInsertBefore(connection.GetStream(), key, before, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LInsertAfter

        public long LInsertAfter(string key, string after, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LInsertAfterCore(
                _encoding.GetBytes(key),
                after != null ? _encoding.GetBytes(after) : null,
                value != null ? _encoding.GetBytes(value) : null);
        }

        public long LInsertAfter(byte[] key, byte[] after, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LInsertAfterCore(key, after, value);
        }

        public async Task<long> LInsertAfterAsync(string key, string after, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LInsertAfterCoreAsync(
                _encoding.GetBytes(key),
               after != null ? _encoding.GetBytes(after) : null,
               value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> LInsertAfterAsync(byte[] key, byte[] after, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LInsertAfterCoreAsync(key, after, value);
        }

        private long LInsertAfterCore(byte[] key, byte[] after, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LInsertAfter(connection.GetStream(), key, after, value);

            return command.Execute();
        }

        private async Task<long> LInsertAfterCoreAsync(byte[] key, byte[] after, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LInsertAfter(connection.GetStream(), key, after, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LLen

        public long LLen(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LLenCore(_encoding.GetBytes(key));
        }

        public long LLen(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LLenCore(key);
        }

        public async Task<long> LLenAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LLenCoreAsync(_encoding.GetBytes(key));
        }

        public async Task<long> LLenAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LLenCoreAsync(key);
        }

        private long LLenCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LLen(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<long> LLenCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LLen(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LPop

        public string LPop(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = LPopCore(_encoding.GetBytes(key));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public byte[] LPop(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LPopCore(key);
        }

        public async Task<string> LPopAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = await LPopCoreAsync(_encoding.GetBytes(key));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public async Task<byte[]> LPopAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LPopCoreAsync(key);
        }

        private byte[] LPopCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPop(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<byte[]> LPopCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPop(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LPush

        public long LPush(string key, string value, params string[] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreValues == null)
            {
                return LPushCore(keyBytes, new[] { value != null ? _encoding.GetBytes(value) : null });
            }

            return LPushCore(keyBytes, moreValues.Prepend(value).Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public long LPush(string key, IEnumerable<string> values)
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

            return LPushCore(_encoding.GetBytes(key), values.Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public long LPush(byte[] key, byte[] value, params byte[][] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (moreValues == null)
            {
                return LPushCore(key, new[] { value });
            }

            return LPushCore(key, moreValues.Prepend(value));
        }

        public long LPush(byte[] key, IEnumerable<byte[]> values)
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

            return LPushCore(key, values);
        }

        public async Task<long> LPushAsync(string key, string value, params string[] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreValues == null)
            {
                return await LPushCoreAsync(keyBytes, new[] { value != null ? _encoding.GetBytes(value) : null });
            }

            return await LPushCoreAsync(keyBytes, moreValues.Prepend(value).Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public async Task<long> LPushAsync(string key, IEnumerable<string> values)
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

            return await LPushCoreAsync(_encoding.GetBytes(key), values.Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public async Task<long> LPushAsync(byte[] key, byte[] value, params byte[][] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (moreValues == null)
            {
                return await LPushCoreAsync(key, new[] { value });
            }

            return await LPushCoreAsync(key, moreValues.Prepend(value));
        }

        public async Task<long> LPushAsync(byte[] key, IEnumerable<byte[]> values)
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

            return await LPushCoreAsync(key, values);
        }

        private long LPushCore(byte[] key, IEnumerable<byte[]> values)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPush(connection.GetStream(), key, values);

            return command.Execute();
        }

        private async Task<long> LPushCoreAsync(byte[] key, IEnumerable<byte[]> values)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPush(connection.GetStream(), key, values);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LPushX

        public long LPushX(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LPushXCore(_encoding.GetBytes(key), value != null ? _encoding.GetBytes(value) : null);
        }

        public long LPushX(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LPushXCore(key, value);
        }

        public async Task<long> LPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LPushXCoreAsync(_encoding.GetBytes(key), value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> LPushXAsync(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LPushXCoreAsync(key, value);
        }

        private long LPushXCore(byte[] key, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPushX(connection.GetStream(), key, value);

            return command.Execute();
        }

        private async Task<long> LPushXCoreAsync(byte[] key, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LPushX(connection.GetStream(), key, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LRange

        public IList<string> LRange(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> valuesBytes = LRangeCore(_encoding.GetBytes(key), start, stop);
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                values[i] = valuesBytes[i] != null ? _encoding.GetString(valuesBytes[i]) : null;
            }

            return values;
        }

        public IList<byte[]> LRange(byte[] key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LRangeCore(key, start, stop);
        }

        public async Task<IList<string>> LRangeAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> valuesBytes = await LRangeCoreAsync(_encoding.GetBytes(key), start, stop);
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                values[i] = valuesBytes[i] != null ? _encoding.GetString(valuesBytes[i]) : null;
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

            return await LRangeCoreAsync(key, start, stop);
        }

        private IList<byte[]> LRangeCore(byte[] key, long start, long stop)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LRange(connection.GetStream(), key, start, stop);

            return command.Execute();
        }

        private async Task<IList<byte[]>> LRangeCoreAsync(byte[] key, long start, long stop)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LRange(connection.GetStream(), key, start, stop);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LRem

        public long LRem(string key, long count, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LRemCore(_encoding.GetBytes(key), count, value != null ? _encoding.GetBytes(value) : null);
        }

        public long LRem(byte[] key, long count, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LRemCore(key, count, value);
        }

        public async Task<long> LRemAsync(string key, long count, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LRemCoreAsync(_encoding.GetBytes(key), count, value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> LRemAsync(byte[] key, long count, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LRemCoreAsync(key, count, value);
        }

        private long LRemCore(byte[] key, long count, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LRem(connection.GetStream(), key, count, value);

            return command.Execute();
        }

        private async Task<long> LRemCoreAsync(byte[] key, long count, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LRem(connection.GetStream(), key, count, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LSet

        public string LSet(string key, long index, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LSetCore(_encoding.GetBytes(key), index, value != null ? _encoding.GetBytes(value) : null);
        }

        public string LSet(byte[] key, long index, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LSetCore(key, index, value);
        }

        public async Task<string> LSetAsync(string key, long index, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LSetCoreAsync(_encoding.GetBytes(key), index, value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<string> LSetAsync(byte[] key, long index, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LSetCoreAsync(key, index, value);
        }

        private string LSetCore(byte[] key, long index, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LSet(connection.GetStream(), key, index, value);

            return command.Execute();
        }

        private async Task<string> LSetCoreAsync(byte[] key, long index, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LSet(connection.GetStream(), key, index, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region LTrim

        public string LTrim(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LTrimCore(_encoding.GetBytes(key), start, stop);
        }

        public string LTrim(byte[] key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return LTrimCore(key, start, stop);
        }

        public async Task<string> LTrimAsync(string key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LTrimCoreAsync(_encoding.GetBytes(key), start, stop);
        }

        public async Task<string> LTrimAsync(byte[] key, long start, long stop)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await LTrimCoreAsync(key, start, stop);
        }

        private string LTrimCore(byte[] key, long start, long stop)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LTrim(connection.GetStream(), key, start, stop);

            return command.Execute();
        }

        private async Task<string> LTrimCoreAsync(byte[] key, long start, long stop)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new LTrim(connection.GetStream(), key, start, stop);

            return await command.ExecuteAsync();
        }

        #endregion

        #region RPop

        public string RPop(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = RPopCore(_encoding.GetBytes(key));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public byte[] RPop(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return RPopCore(key);
        }

        public async Task<string> RPopAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] valueBytes = await RPopCoreAsync(_encoding.GetBytes(key));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public async Task<byte[]> RPopAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await RPopCoreAsync(key);
        }

        private byte[] RPopCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPop(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<byte[]> RPopCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPop(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region RPush

        public long RPush(string key, string value, params string[] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreValues == null)
            {
                return RPushCore(keyBytes, new[] { value != null ? _encoding.GetBytes(value) : null });
            }

            return RPushCore(keyBytes, moreValues.Prepend(value).Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public long RPush(string key, IEnumerable<string> values)
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

            return RPushCore(_encoding.GetBytes(key), values.Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public long RPush(byte[] key, byte[] value, params byte[][] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (moreValues == null)
            {
                return RPushCore(key, new[] { value });
            }

            return RPushCore(key, moreValues.Prepend(value));
        }

        public long RPush(byte[] key, IEnumerable<byte[]> values)
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

            return RPushCore(key, values);
        }

        public async Task<long> RPushAsync(string key, string value, params string[] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreValues == null)
            {
                return await RPushCoreAsync(keyBytes, new[] { value != null ? _encoding.GetBytes(value) : null });
            }

            return await RPushCoreAsync(keyBytes, moreValues.Prepend(value).Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public async Task<long> RPushAsync(string key, IEnumerable<string> values)
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

            return await RPushCoreAsync(_encoding.GetBytes(key), values.Select(x => x != null ? _encoding.GetBytes(x) : null));
        }

        public async Task<long> RPushAsync(byte[] key, byte[] value, params byte[][] moreValues)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (moreValues == null)
            {
                return await RPushCoreAsync(key, new[] { value });
            }

            return await RPushCoreAsync(key, moreValues.Prepend(value));
        }

        public async Task<long> RPushAsync(byte[] key, IEnumerable<byte[]> values)
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

            return await RPushCoreAsync(key, values);
        }

        private long RPushCore(byte[] key, IEnumerable<byte[]> values)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPush(connection.GetStream(), key, values);

            return command.Execute();
        }

        private async Task<long> RPushCoreAsync(byte[] key, IEnumerable<byte[]> values)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPush(connection.GetStream(), key, values);

            return await command.ExecuteAsync();
        }

        #endregion

        #region RPushX

        public long RPushX(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return RPushXCore(_encoding.GetBytes(key), value != null ? _encoding.GetBytes(value) : null);
        }

        public long RPushX(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return RPushXCore(key, value);
        }

        public async Task<long> RPushXAsync(string key, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await RPushXCoreAsync(_encoding.GetBytes(key), value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> RPushXAsync(byte[] key, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await RPushXCoreAsync(key, value);
        }

        private long RPushXCore(byte[] key, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPushX(connection.GetStream(), key, value);

            return command.Execute();
        }

        private async Task<long> RPushXCoreAsync(byte[] key, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPushX(connection.GetStream(), key, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region RPopLPush

        public string RPopLPush(string source, string destination)
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

            byte[] valueBytes = RPopLPushCore(_encoding.GetBytes(source), _encoding.GetBytes(destination));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public byte[] RPopLPush(byte[] source, byte[] destination)
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

            return RPopLPushCore(source, destination);
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

            byte[] valueBytes = await RPopLPushCoreAsync(_encoding.GetBytes(source), _encoding.GetBytes(destination));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public async Task<byte[]> RPopLPushAsync(byte[] source, byte[] destination)
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

            return await RPopLPushCoreAsync(source, destination);
        }

        private byte[] RPopLPushCore(byte[] source, byte[] destination)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPopLPush(connection.GetStream(), source, destination);

            return command.Execute();
        }

        private async Task<byte[]> RPopLPushCoreAsync(byte[] source, byte[] destination)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new RPopLPush(connection.GetStream(), source, destination);

            return await command.ExecuteAsync();
        }

        #endregion
    }
}
