using Biscuits.Redis.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region HDel

        public long HDel(string key, params string[] fields)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            var fieldsBytes = new List<byte[]>(fields.Length);

            for (int i = 0; i < fields.Length; i++)
            {
                if (fields[i] == null)
                    throw new ArgumentException(nameof(fields));

                fieldsBytes.Add(_encoding.GetBytes(fields[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return HDel(keyBytes, fieldsBytes);
        }

        public long HDel(byte[] key, ICollection<byte[]> fields)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            if (fields.Count == 0)
                throw new ArgumentException(nameof(fields));

            if (fields.Any(x => x == null))
                throw new ArgumentException(nameof(fields));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HDel(connection.GetStream(), key, fields);
                return command.Execute();
            }
        }

        public async Task<long> HDelasync(string key, params string[] fields)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            var fieldsBytes = new List<byte[]>(fields.Length);

            for (int i = 0; i < fields.Length; i++)
            {
                if (fields[i] == null)
                    throw new ArgumentException(nameof(fields));

                fieldsBytes.Add(_encoding.GetBytes(fields[i]));
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await HDelAsync(keyBytes, fieldsBytes);
        }

        public async Task<long> HDelAsync(byte[] key, ICollection<byte[]> values)
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
                var command = new HDel(connection.GetStream(), key, values);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HExists

        public long HExists(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);

            return HExists(keyBytes, fieldBytes);
        }

        public long HExists(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HExists(connection.GetStream(), key, field);
                return command.Execute();
            }
        }

        public async Task<long> HExistsAsync(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);

            return await HExistsAsync(keyBytes, fieldBytes);
        }

        public async Task<long> HExistsAsync(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HExists(connection.GetStream(), key, field);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HGet

        public string HGet(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = HGet(keyBytes, fieldBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public byte[] HGet(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HGet(connection.GetStream(), key, field);
                return command.Execute();
            }
        }

        public async Task<string> HGetAsync(string key, string field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = await HGetAsync(keyBytes, fieldBytes);

            if (valueBytes == null)
                return null;

            return _encoding.GetString(valueBytes);
        }

        public async Task<byte[]> HGetAsync(byte[] key, byte[] field)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HGet(connection.GetStream(), key, field);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HGetAll

        public IList<KeyValuePair<string, string>> HGetAll(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<KeyValuePair<byte[], byte[]>> fieldValuePairsBytes = HGetAll(keyBytes);

            var fieldValuePairs = new List<KeyValuePair<string, string>>(fieldValuePairsBytes.Count);

            for (int i = 0; i < fieldValuePairsBytes.Count; i++)
            {
                string field = _encoding.GetString(fieldValuePairsBytes[i].Key);
                string value = _encoding.GetString(fieldValuePairsBytes[i].Value);

                fieldValuePairs.Add(new KeyValuePair<string, string>(field, value));
            }

            return fieldValuePairs;
        }

        public IList<KeyValuePair<byte[], byte[]>> HGetAll(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            IList<byte[]> fieldsAndValues;

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HGetAll(connection.GetStream(), key);
                fieldsAndValues = command.Execute();
            }

            var fieldValuePairs = new List<KeyValuePair<byte[], byte[]>>(fieldsAndValues.Count / 2);

            for (int i = 0; i < fieldsAndValues.Count; i += 2)
            {
                fieldValuePairs.Add(new KeyValuePair<byte[], byte[]>(fieldsAndValues[i], fieldsAndValues[i + 1]));
            }

            return fieldValuePairs;
        }

        public async Task<IList<KeyValuePair<string, string>>> HGetAllAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<KeyValuePair<byte[], byte[]>> fieldValuePairsBytes = await HGetAllAsync(keyBytes);

            var fieldValuePairs = new List<KeyValuePair<string, string>>(fieldValuePairsBytes.Count);

            for (int i = 0; i < fieldValuePairsBytes.Count; i++)
            {
                string field = _encoding.GetString(fieldValuePairsBytes[i].Key);
                string value = _encoding.GetString(fieldValuePairsBytes[i].Value);

                fieldValuePairs.Add(new KeyValuePair<string, string>(field, value));
            }

            return fieldValuePairs;
        }

        public async Task<IList<KeyValuePair<byte[], byte[]>>> HGetAllAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            IList<byte[]> fieldsAndValues;

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HGetAll(connection.GetStream(), key);
                fieldsAndValues = await command.ExecuteAsync();
            }

            var fieldValuePairs = new List<KeyValuePair<byte[], byte[]>>(fieldsAndValues.Count / 2);

            for (int i = 0; i < fieldsAndValues.Count; i += 2)
            {
                var fieldValuePair = new KeyValuePair<byte[], byte[]>(fieldsAndValues[i], fieldsAndValues[i + 1]);
                fieldValuePairs.Add(fieldValuePair);
            }

            return fieldValuePairs;
        }

        #endregion

        #region HKeys

        public IList<string> HKeys(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> fieldsBytes = HKeys(keyBytes);

            var fields = new List<string>(fieldsBytes.Count);

            for (int i = 0; i < fieldsBytes.Count; i++)
            {
                string field = _encoding.GetString(fieldsBytes[i]);
                fields.Add(field);
            }

            return fields;
        }

        public IList<byte[]> HKeys(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HKeys(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<IList<string>> HKeysAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> fieldsBytes = await HKeysAsync(keyBytes);

            var fields = new List<string>(fieldsBytes.Count);

            for (int i = 0; i < fieldsBytes.Count; i++)
            {
                string field = _encoding.GetString(fieldsBytes[i]);
                fields.Add(field);
            }

            return fields;
        }

        public async Task<IList<byte[]>> HKeysAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HKeys(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HLen

        public long HLen(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return HLen(keyBytes);
        }

        public long HLen(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HLen(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<long> HLenAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            return await HLenAsync(keyBytes);
        }

        public async Task<long> HLenAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HLen(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion
        
        #region HMSet

        public string HMSet(string key, ICollection<KeyValuePair<string, string>> fieldValuePairs)
        {
            ValidateNotDisposed();
            
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (fieldValuePairs == null)
                throw new ArgumentNullException(nameof(fieldValuePairs));

            var fieldValuePairsBytes = new List<KeyValuePair<byte[], byte[]>>(fieldValuePairs.Count);

            foreach (KeyValuePair<string, string> fieldValuePair in fieldValuePairs)
            {
                byte[] fieldBytes = _encoding.GetBytes(fieldValuePair.Key);
                byte[] valueBytes = fieldValuePair.Value != null ? _encoding.GetBytes(fieldValuePair.Value) : null;

                var fieldValuePairBytes = new KeyValuePair<byte[], byte[]>(fieldBytes, valueBytes);
                fieldValuePairsBytes.Add(fieldValuePairBytes);
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return HMSet(keyBytes, fieldValuePairsBytes);
        }

        public string HMSet(byte[] key, ICollection<KeyValuePair<byte[], byte[]>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (fieldValuePairs == null)
                throw new ArgumentNullException(nameof(fieldValuePairs));

            var fieldsAndValues = new List<byte[]>(fieldValuePairs.Count * 2);

            foreach (KeyValuePair<byte[], byte[]> fieldValuePair in fieldValuePairs)
            {
                fieldsAndValues.Add(fieldValuePair.Key);
                fieldsAndValues.Add(fieldValuePair.Value);
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HMSet(connection.GetStream(), key, fieldsAndValues);
                return command.Execute();
            }
        }

        public async Task<string> HMSetAsync(string key, ICollection<KeyValuePair<string, string>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (fieldValuePairs == null)
                throw new ArgumentNullException(nameof(fieldValuePairs));

            var fieldValuePairsBytes = new List<KeyValuePair<byte[], byte[]>>(fieldValuePairs.Count);

            foreach (KeyValuePair<string, string> fieldValuePair in fieldValuePairs)
            {
                byte[] fieldBytes = _encoding.GetBytes(fieldValuePair.Key);
                byte[] valueBytes = fieldValuePair.Value != null ? _encoding.GetBytes(fieldValuePair.Value) : null;

                var fieldValuePairBytes = new KeyValuePair<byte[], byte[]>(fieldBytes, valueBytes);
                fieldValuePairsBytes.Add(fieldValuePairBytes);
            }

            byte[] keyBytes = _encoding.GetBytes(key);
            return await HMSetAsync(keyBytes, fieldValuePairsBytes);
        }

        public async Task<string> HMSetAsync(byte[] key, ICollection<KeyValuePair<byte[], byte[]>> fieldValuePairs)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (fieldValuePairs == null)
                throw new ArgumentNullException(nameof(fieldValuePairs));

            var fieldsAndValues = new List<byte[]>(fieldValuePairs.Count * 2);

            foreach (KeyValuePair<byte[], byte[]> fieldValuePair in fieldValuePairs)
            {
                fieldsAndValues.Add(fieldValuePair.Key);
                fieldsAndValues.Add(fieldValuePair.Value);
            }

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HMSet(connection.GetStream(), key, fieldsAndValues);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HSet

        public long HSet(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = value != null ? _encoding.GetBytes(value) : null;

            return HSet(keyBytes, fieldBytes, valueBytes);
        }

        public long HSet(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HSet(connection.GetStream(), key, field, value);
                return command.Execute();
            }
        }

        public async Task<long> HSetAsync(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = value != null ? _encoding.GetBytes(value) : null;

            return await HSetAsync(keyBytes, fieldBytes, valueBytes);
        }

        public async Task<long> HSetAsync(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HSet(connection.GetStream(), key, field, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HSetNX

        public long HSetNX(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = value != null ? _encoding.GetBytes(value) : null;

            return HSetNX(keyBytes, fieldBytes, valueBytes);
        }

        public long HSetNX(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HSetNX(connection.GetStream(), key, field, value);
                return command.Execute();
            }
        }

        public async Task<long> HSetNXAsync(string key, string field, string value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            byte[] keyBytes = _encoding.GetBytes(key);
            byte[] fieldBytes = _encoding.GetBytes(field);
            byte[] valueBytes = value != null ? _encoding.GetBytes(value) : null;

            return await HSetNXAsync(keyBytes, fieldBytes, valueBytes);
        }

        public async Task<long> HSetNXAsync(byte[] key, byte[] field, byte[] value)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            if (field == null)
                throw new ArgumentNullException(nameof(field));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HSetNX(connection.GetStream(), key, field, value);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region HVals

        public IList<string> HVals(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes = HVals(keyBytes);

            var values = new List<string>(valuesBytes.Count);

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                string value = _encoding.GetString(valuesBytes[i]);
                values.Add(value);
            }

            return values;
        }

        public IList<byte[]> HVals(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HVals(connection.GetStream(), key);
                return command.Execute();
            }
        }

        public async Task<IList<string>> HValsAsync(string key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] keyBytes = _encoding.GetBytes(key);
            IList<byte[]> valuesBytes = await HValsAsync(keyBytes);

            var values = new List<string>(valuesBytes.Count);

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                string value = _encoding.GetString(valuesBytes[i]);
                values.Add(value);
            }

            return values;
        }

        public async Task<IList<byte[]>> HValsAsync(byte[] key)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key.Length == 0)
                throw new ArgumentException(nameof(key));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new HVals(connection.GetStream(), key);
                return await command.ExecuteAsync();
            }
        }

        #endregion
    }
}
