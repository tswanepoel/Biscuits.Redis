using Biscuits.Redis.Commands.Hashes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region HDel

        public long HDel(string key, string field, params string[] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreFields == null)
            {
                return HDelCore(keyBytes, new[] { _encoding.GetBytes(field) });
            }

            return HDelCore(keyBytes, moreFields.Prepend(field).Select(_encoding.GetBytes));
        }

        public long HDel(string key, IEnumerable<string> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return HDelCore(_encoding.GetBytes(key), fields.Select(_encoding.GetBytes));
        }

        public long HDel(byte[] key, byte[] field, params byte[][] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (moreFields == null)
            {
                return HDelCore(key, new[] { field });
            }

            return HDelCore(key, moreFields.Prepend(field));
        }

        public long HDel(byte[] key, IEnumerable<byte[]> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return HDelCore(key, fields);
        }

        public async Task<long> HDelAsync(string key, string field, params string[] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] keyBytes = _encoding.GetBytes(key);

            if (moreFields == null)
            {
                return await HDelCoreAsync(keyBytes, new[] { _encoding.GetBytes(field) });
            }

            return await HDelCoreAsync(keyBytes, moreFields.Prepend(field).Select(_encoding.GetBytes));
        }

        public async Task<long> HDelAsync(string key, IEnumerable<string> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return await HDelCoreAsync(_encoding.GetBytes(key), fields.Select(_encoding.GetBytes));
        }

        public async Task<long> HDelAsync(byte[] key, byte[] field, params byte[][] moreFields)
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

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (moreFields == null)
            {
                return await HDelCoreAsync(key, new[] { field });
            }

            return await HDelCoreAsync(key, moreFields.Prepend(field));
        }

        public async Task<long> HDelAsync(byte[] key, IEnumerable<byte[]> fields)
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

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return await HDelCoreAsync(key, fields);
        }

        private long HDelCore(byte[] key, IEnumerable<byte[]> fields)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HDel(connection.GetStream(), key, fields);

            return command.Execute();
        }

        private async Task<long> HDelCoreAsync(byte[] key, IEnumerable<byte[]> fields)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HDel(connection.GetStream(), key, fields);
            
            return await command.ExecuteAsync();
        }

        #endregion

        #region HExists

        public long HExists(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HExistsCore(_encoding.GetBytes(key), _encoding.GetBytes(field));
        }

        public long HExists(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HExistsCore(key, field);
        }

        public async Task<long> HExistsAsync(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HExistsCoreAsync(_encoding.GetBytes(key), _encoding.GetBytes(field));
        }

        public async Task<long> HExistsAsync(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HExistsCoreAsync(key, field);
        }

        private long HExistsCore(byte[] key, byte[] field)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HExists(connection.GetStream(), key, field);

            return command.Execute();
        }

        private async Task<long> HExistsCoreAsync(byte[] key, byte[] field)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HExists(connection.GetStream(), key, field);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HGet

        public string HGet(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] valueBytes = HGetCore(_encoding.GetBytes(key), _encoding.GetBytes(field));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public byte[] HGet(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HGetCore(key, field);
        }

        public async Task<string> HGetAsync(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] valueBytes = await HGetCoreAsync(_encoding.GetBytes(key), _encoding.GetBytes(field));
            return valueBytes != null ? _encoding.GetString(valueBytes) : null;
        }

        public async Task<byte[]> HGetAsync(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HGetCoreAsync(key, field);
        }

        private byte[] HGetCore(byte[] key, byte[] field)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HGet(connection.GetStream(), key, field);

            return command.Execute();
        }

        private async Task<byte[]> HGetCoreAsync(byte[] key, byte[] field)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HGet(connection.GetStream(), key, field);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HGetAll

        public IList<KeyValuePair<string, string>> HGetAll(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> values = HGetAllCore(_encoding.GetBytes(key));
            var pairs = new KeyValuePair<string, string>[values.Count / 2];

            for (var i = 0; i < pairs.Length; i++)
            {
                byte[] value = values[2 * i + 1];

                pairs[i] = KeyValuePair.Create(
                    _encoding.GetString(values[2 * i]),
                    value != null ? _encoding.GetString(value) : null);
            }

            return pairs;
        }

        public IList<KeyValuePair<byte[], byte[]>> HGetAll(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> values = HGetAllCore(key);
            var pairs = new KeyValuePair<byte[], byte[]>[values.Count / 2];

            for (var i = 0; i < pairs.Length; i++)
            {
                pairs[i] = KeyValuePair.Create(values[2 * i], values[2 * i + 1]);
            }

            return pairs;
        }

        public async Task<IList<KeyValuePair<string, string>>> HGetAllAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> values = await HGetAllCoreAsync(_encoding.GetBytes(key));
            var pairs = new KeyValuePair<string, string>[values.Count / 2];

            for (var i = 0; i < pairs.Length; i++)
            {
                byte[] value = values[2 * i + 1];

                pairs[i] = KeyValuePair.Create(
                    _encoding.GetString(values[2 * i]),
                    value != null ? _encoding.GetString(value) : null);
            }

            return pairs;
        }

        public async Task<IList<KeyValuePair<byte[], byte[]>>> HGetAllAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> values = await HGetAllCoreAsync(key);
            var pairs = new KeyValuePair<byte[], byte[]>[values.Count / 2];

            for (var i = 0; i < pairs.Length; i++)
            {
                pairs[i] = KeyValuePair.Create(values[2 * i], values[2 * i + 1]);
            }

            return pairs;
        }

        private IList<byte[]> HGetAllCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HGetAll(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<IList<byte[]>> HGetAllCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HGetAll(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HIncrBy

        public long HIncrBy(string key, string field, long value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HIncrByCore(_encoding.GetBytes(key), _encoding.GetBytes(field), value);
        }

        public long HIncrBy(byte[] key, byte[] field, long value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HIncrByCore(key, field, value);
        }

        public async Task<long> HIncrByAsync(string key, string field, long value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HIncrByCoreAsync(_encoding.GetBytes(key), _encoding.GetBytes(field), value);
        }

        public async Task<long> HIncrByAsync(byte[] key, byte[] field, long value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HIncrByCoreAsync(key, field, value);
        }

        private long HIncrByCore(byte[] key, byte[] field, long value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HIncrBy(connection.GetStream(), key, field, value);

            return command.Execute();
        }

        private async Task<long> HIncrByCoreAsync(byte[] key, byte[] field, long value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HIncrBy(connection.GetStream(), key, field, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HKeys

        public IList<string> HKeys(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> fieldsBytes = HKeysCore(_encoding.GetBytes(key));
            var fields = new string[fieldsBytes.Count];

            for (var i = 0; i < fields.Length; i++)
            {
                fields[i] = _encoding.GetString(fieldsBytes[i]);
            }

            return fields;
        }

        public IList<byte[]> HKeys(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return HKeysCore(key);
        }

        public async Task<IList<string>> HKeysAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> fieldsBytes = await HKeysCoreAsync(_encoding.GetBytes(key));
            var fields = new string[fieldsBytes.Count];

            for (var i = 0; i < fields.Length; i++)
            {
                fields[i] = _encoding.GetString(fieldsBytes[i]);
            }

            return fields;
        }

        public async Task<IList<byte[]>> HKeysAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await HKeysCoreAsync(key);
        }

        private IList<byte[]> HKeysCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HKeys(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<IList<byte[]>> HKeysCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HKeys(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HLen

        public long HLen(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return HLenCore(_encoding.GetBytes(key));
        }

        public long HLen(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return HLenCore(key);
        }

        public async Task<long> HLenAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await HLenCoreAsync(_encoding.GetBytes(key));
        }

        public async Task<long> HLenAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await HLenCoreAsync(key);
        }

        private long HLenCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HLen(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<long> HLenCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HLen(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HMGet

        public IList<string> HMGet(string key, string field, params string[] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes;

            if (moreFields == null)
            {
                valuesBytes = HMGetCore(keyBytes, new[] { _encoding.GetBytes(field) });
            }

            valuesBytes = HMGetCore(keyBytes, moreFields.Prepend(field).Select(_encoding.GetBytes));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public IList<string> HMGet(string key, IEnumerable<string> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            IList<byte[]> valuesBytes = HMGetCore(_encoding.GetBytes(key), fields.Select(_encoding.GetBytes));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public IList<byte[]> HMGet(byte[] key, byte[] field, params byte[][] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (moreFields == null)
            {
                return HMGetCore(key, new[] { field });
            }

            return HMGetCore(key, moreFields.Prepend(field));
        }

        public IList<byte[]> HMGet(byte[] key, IEnumerable<byte[]> fields)
        {
            ValidateNotDisposed();
            
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return HMGetCore(key, fields);
        }

        public async Task<IList<string>> HMGetAsync(string key, string field, params string[] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes;

            if (moreFields == null)
            {
                valuesBytes = await HMGetCoreAsync(keyBytes, new[] { _encoding.GetBytes(field) });
            }

            valuesBytes = await HMGetCoreAsync(keyBytes, moreFields.Prepend(field).Select(_encoding.GetBytes));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public async Task<IList<string>> HMGetAsync(string key, IEnumerable<string> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            IList<byte[]> valuesBytes = await HMGetCoreAsync(_encoding.GetBytes(key), fields.Select(_encoding.GetBytes));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public async Task<IList<byte[]>> HMGetAsync(byte[] key, byte[] field, params byte[][] moreFields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (moreFields == null)
            {
                return await HMGetCoreAsync(key, new[] { field });
            }

            return await HMGetCoreAsync(key, moreFields.Prepend(field));
        }

        public async Task<IList<byte[]>> HMGetAsync(byte[] key, IEnumerable<byte[]> fields)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            return await HMGetCoreAsync(key, fields);
        }

        private IList<byte[]> HMGetCore(byte[] key, IEnumerable<byte[]> fields)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HMGet(connection.GetStream(), key, fields);

            return command.Execute();
        }

        private async Task<IList<byte[]>> HMGetCoreAsync(byte[] key, IEnumerable<byte[]> fields)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HMGet(connection.GetStream(), key, fields);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HMSet

        public string HMSet(string key, IEnumerable<KeyValuePair<string, string>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fieldValuePairs == null)
            {
                throw new ArgumentNullException(nameof(fieldValuePairs));
            }

            IEnumerable<byte[]> fieldsAndValues = fieldValuePairs.SelectMany(x =>
                new[]
                {
                    _encoding.GetBytes(x.Key),
                    x.Value != null ? _encoding.GetBytes(x.Value) : null
                });

            return HMSetCore(_encoding.GetBytes(key), fieldsAndValues);
        }

        public string HMSet(byte[] key, IEnumerable<KeyValuePair<byte[], byte[]>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fieldValuePairs == null)
            {
                throw new ArgumentNullException(nameof(fieldValuePairs));
            }

            return HMSetCore(key, fieldValuePairs.SelectMany(x => new[] { x.Key, x.Value }));
        }

        public async Task<string> HMSetAsync(string key, IEnumerable<KeyValuePair<string, string>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fieldValuePairs == null)
            {
                throw new ArgumentNullException(nameof(fieldValuePairs));
            }

            IEnumerable<byte[]> fieldsAndValues = fieldValuePairs.SelectMany(x =>
                new[]
                {
                    _encoding.GetBytes(x.Key),
                    x.Value != null ? _encoding.GetBytes(x.Value) : null
                });

            return await HMSetCoreAsync(_encoding.GetBytes(key), fieldsAndValues);
        }

        public async Task<string> HMSetAsync(byte[] key, IEnumerable<KeyValuePair<byte[], byte[]>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (fieldValuePairs == null)
            {
                throw new ArgumentNullException(nameof(fieldValuePairs));
            }

            return await HMSetCoreAsync(key, fieldValuePairs.SelectMany(x => new[] { x.Key, x.Value }));
        }

        private string HMSetCore(byte[] key, IEnumerable<byte[]> fieldsAndValues)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HMSet(connection.GetStream(), key, fieldsAndValues);

            return command.Execute();
        }

        private async Task<string> HMSetCoreAsync(byte[] key, IEnumerable<byte[]> fieldsAndValues)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HMSet(connection.GetStream(), key, fieldsAndValues);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HSet

        public long HSet(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HSetCore(
                _encoding.GetBytes(key), 
                _encoding.GetBytes(field), 
                value != null ? _encoding.GetBytes(value) : null);
        }

        public long HSet(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HSetCore(key, field, value);
        }

        public async Task<long> HSetAsync(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HSetCoreAsync(
                _encoding.GetBytes(key), 
                _encoding.GetBytes(field), 
                value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> HSetAsync(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HSetCoreAsync(key, field, value);
        }

        private async Task<long> HSetCoreAsync(byte[] key, byte[] field, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HSet(connection.GetStream(), key, field, value);

            return await command.ExecuteAsync();
        }

        private long HSetCore(byte[] key, byte[] field, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HSet(connection.GetStream(), key, field, value);

            return command.Execute();
        }

        #endregion

        #region HSetNX

        public long HSetNX(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HSetNXCore(
                _encoding.GetBytes(key),
                _encoding.GetBytes(field),
                value != null ? _encoding.GetBytes(value) : null);
        }

        public long HSetNX(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return HSetNXCore(key, field, value);
        }

        public async Task<long> HSetNXAsync(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            return await HSetNXCoreAsync(
                _encoding.GetBytes(key), 
                _encoding.GetBytes(field), 
                value != null ? _encoding.GetBytes(value) : null);
        }

        public async Task<long> HSetNXAsync(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }
            
            return await HSetNXCoreAsync(key, field, value);
        }

        private long HSetNXCore(byte[] key, byte[] field, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HSetNX(connection.GetStream(), key, field, value);

            return command.Execute();
        }

        private async Task<long> HSetNXCoreAsync(byte[] key, byte[] field, byte[] value)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HSetNX(connection.GetStream(), key, field, value);

            return await command.ExecuteAsync();
        }

        #endregion

        #region HVals

        public IList<string> HVals(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> valuesBytes = HValsCore(_encoding.GetBytes(key));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < valuesBytes.Count; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public IList<byte[]> HVals(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return HValsCore(key);
        }

        public async Task<IList<string>> HValsAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            IList<byte[]> valuesBytes = await HValsCoreAsync(_encoding.GetBytes(key));
            var values = new string[valuesBytes.Count];

            for (var i = 0; i < values.Length; i++)
            {
                if (valuesBytes[i] != null)
                {
                    values[i] = _encoding.GetString(valuesBytes[i]);
                }
            }

            return values;
        }

        public async Task<IList<byte[]>> HValsAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return await HValsCoreAsync(key);
        }

        private IList<byte[]> HValsCore(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HVals(connection.GetStream(), key);

            return command.Execute();
        }

        private async Task<IList<byte[]>> HValsCoreAsync(byte[] key)
        {
            using var connection = new RedisConnection(_connectionSettings);
            var command = new HVals(connection.GetStream(), key);

            return await command.ExecuteAsync();
        }

        #endregion
    }
}
