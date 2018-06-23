using Biscuits.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Biscuits.Collections.Redis
{
    public class RedisDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IReadOnlyDictionary<TKey, TValue>
    {
        readonly RedisClient _client;
        readonly byte[] _key;
        readonly IEqualityComparer<TValue> _valueComparer;

        public RedisDictionary(RedisClient client, byte[] key)
            : this(client, key, EqualityComparer<TValue>.Default)
        {
        }

        public RedisDictionary(RedisClient client, byte[] key, IEqualityComparer<TValue> valueComparer)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _key = key ?? throw new ArgumentNullException(nameof(key));
            _valueComparer = valueComparer ?? throw new ArgumentNullException(nameof(valueComparer));
        }

        public TValue this[TKey key]
        {
            get { return GetValue(key); }
            set { AddOrSetValue(key, value); }
        }

        public TValue GetValue(TKey key)
        {
            if (TryGetValue(key, out TValue value))
                return value;

            throw new KeyNotFoundException("The given key was not present in the dictionary.");
        }

        public async Task<TValue> GetValueAsync(TKey key)
        {
            (bool success, TValue value) = await TryGetValueAsync(key);

            if (success)
                return value;

            throw new KeyNotFoundException("The given key was not present in the dictionary.");
        }

        public void AddOrSetValue(TKey key, TValue value)
        {
            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = value != null ? BsonConvert.SerializeObject(value) : null;

            _client.HSet(_key, fieldBytes, valueBytes);
        }

        public async Task AddOrSetValueAsync(TKey key, TValue value)
        {
            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = value != null ? BsonConvert.SerializeObject(value) : null;

            await _client.HSetAsync(_key, fieldBytes, valueBytes);
        }

        public void AddOrSetValue(ICollection<KeyValuePair<TKey, TValue>> items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var fieldValuePairs = new List<KeyValuePair<byte[], byte[]>>(items.Count);

            foreach (KeyValuePair<TKey, TValue> item in items)
            {
                byte[] fieldBytes = BsonConvert.SerializeObject(item.Key);
                byte[] valueBytes = item.Value != null ? BsonConvert.SerializeObject(item.Value) : null;

                fieldValuePairs.Add(new KeyValuePair<byte[], byte[]>(fieldBytes, valueBytes));
            }

            _client.HMSet(_key, fieldValuePairs);
        }

        public async Task AddOrSetValueAsync(ICollection<KeyValuePair<TKey, TValue>> items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var fieldValuePairs = new List<KeyValuePair<byte[], byte[]>>(items.Count);

            foreach (KeyValuePair<TKey, TValue> item in items)
            {
                byte[] fieldBytes = BsonConvert.SerializeObject(item.Key);
                byte[] valueBytes = item.Value != null ? BsonConvert.SerializeObject(item.Value) : null;

                fieldValuePairs.Add(new KeyValuePair<byte[], byte[]>(fieldBytes, valueBytes));
            }

            await _client.HMSetAsync(_key, fieldValuePairs);
        }

        public ICollection<TKey> Keys
        {
            get { return GetKeys(); }
        }

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys
        {
            get { return GetKeys(); }
        }

        public ICollection<TValue> Values
        {
            get { return GetValues(); }
        }

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values
        {
            get { return GetValues(); }
        }

        public ICollection<TKey> GetKeys()
        {
            IList<byte[]> fieldsBytes = _client.HKeys(_key);
            var fields = new TKey[fieldsBytes.Count];

            for (int i = 0; i < fieldsBytes.Count; i++)
            {
                fields[i] = BsonConvert.DeserializeObject<TKey>(fieldsBytes[i]);
            }

            return fields;
        }

        public async Task<ICollection<TKey>> GetKeysAsync()
        {
            IList<byte[]> fieldsBytes = await _client.HKeysAsync(_key);
            var fields = new TKey[fieldsBytes.Count];

            for (int i = 0; i < fieldsBytes.Count; i++)
            {
                fields[i] = BsonConvert.DeserializeObject<TKey>(fieldsBytes[i]);
            }

            return fields;
        }

        public ICollection<TValue> GetValues()
        {
            IList<byte[]> valuesBytes = _client.HVals(_key);
            var values = new TValue[valuesBytes.Count];

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                values[i] = BsonConvert.DeserializeObject<TValue>(valuesBytes[i]);
            }

            return values;
        }

        public async Task<ICollection<TValue>> GetValuesAsync()
        {
            IList<byte[]> valuesBytes = await _client.HValsAsync(_key);
            var values = new TValue[valuesBytes.Count];

            for (int i = 0; i < valuesBytes.Count; i++)
            {
                values[i] = BsonConvert.DeserializeObject<TValue>(valuesBytes[i]);
            }

            return values;
        }

        public int Count
        {
            get { return (int)GetCount(); }
        }

        public long GetCount()
        {
            return _client.HLen(_key);
        }

        public async Task<long> GetCountAsync()
        {
            return await _client.HLenAsync(_key);
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public void Add(TKey key, TValue value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (ContainsKey(key))
                throw new ArgumentException("An item with the same key has already been added.");

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = value != null ? BsonConvert.SerializeObject(value) : null;

            _client.HSet(_key, fieldBytes, valueBytes);
        }

        public async Task AddAsync(TKey key, TValue value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (await ContainsKeyAsync(key))
                throw new ArgumentException("An item with the same key has already been added.");

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = value != null ? BsonConvert.SerializeObject(value) : null;

            await _client.HSetAsync(_key, fieldBytes, valueBytes);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public async Task AddAsync(KeyValuePair<TKey, TValue> item)
        {
            await AddAsync(item.Key, item.Value);
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public async Task ClearAsync()
        {
            throw new NotImplementedException();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return TryGetValue(item.Key, out TValue value) && _valueComparer.Equals(item.Value, value);
        }

        public async Task<bool> ContainsAsync(KeyValuePair<TKey, TValue> item)
        {
            (bool success, TValue value) = await TryGetValueAsync(item.Key);
            return success && _valueComparer.Equals(item.Value, value);
        }

        public bool ContainsKey(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            return _client.HExists(_key, fieldBytes) != 0L;
        }

        public async Task<bool> ContainsKeyAsync(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            return await _client.HExistsAsync(_key, fieldBytes) != 0L;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }

            if (index < 0 || index > array.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            long count = GetCount();

            if (array.Length - index < count)
            {
                throw new ArgumentException("Destination array is not long enough.", nameof(array));
            }

            foreach (KeyValuePair<TKey, TValue> item in this)
            {
                array[index++] = item;
            }
        }

        public ICollection<KeyValuePair<TKey, TValue>> GetAll()
        {
            IList<KeyValuePair<byte[], byte[]>> fieldValuePairs = _client.HGetAll(_key);
            var items = new KeyValuePair<TKey, TValue>[fieldValuePairs.Count];

            for (int i = 0; i < fieldValuePairs.Count; i++)
            {
                TKey key = BsonConvert.DeserializeObject<TKey>(fieldValuePairs[i].Key);
                TValue value = BsonConvert.DeserializeObject<TValue>(fieldValuePairs[i].Value);

                items[i] = new KeyValuePair<TKey, TValue>(key, value);
            }

            return items;
        }

        public async Task<ICollection<KeyValuePair<TKey, TValue>>> GetAllAsync()
        {
            IList<KeyValuePair<byte[], byte[]>> fieldValuePairs = await _client.HGetAllAsync(_key);
            var items = new KeyValuePair<TKey, TValue>[fieldValuePairs.Count];

            for (int i = 0; i < fieldValuePairs.Count; i++)
            {
                TKey key = BsonConvert.DeserializeObject<TKey>(fieldValuePairs[i].Key);
                TValue value = BsonConvert.DeserializeObject<TValue>(fieldValuePairs[i].Value);

                items[i] = new KeyValuePair<TKey, TValue>(key, value);
            }

            return items;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            KeyValuePair<TKey, TValue> Convert(KeyValuePair<byte[], byte[]> fieldValuePair)
            {
                TKey key = BsonConvert.DeserializeObject<TKey>(fieldValuePair.Key);
                TValue value = BsonConvert.DeserializeObject<TValue>(fieldValuePair.Value);

                return new KeyValuePair<TKey, TValue>(key, value);
            }

            return _client.HGetAll(_key).Select(Convert).GetEnumerator();
        }
        
        public bool Remove(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            return _client.HDel(_key, new[] { fieldBytes }) != 0L;
        }

        public async Task<bool> RemoveAsync(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            return await _client.HDelAsync(_key, new[] { fieldBytes }) != 0L;
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        public async Task<bool> RemoveAsync(KeyValuePair<TKey, TValue> item)
        {
            return await RemoveAsync(item.Key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = _client.HGet(_key, fieldBytes);

            if (valueBytes == null)
            {
                value = default(TValue);
                return false;
            }

            value = BsonConvert.DeserializeObject<TValue>(valueBytes);
            return true;
        }

        public async Task<(bool, TValue)> TryGetValueAsync(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            byte[] fieldBytes = BsonConvert.SerializeObject(key);
            byte[] valueBytes = await _client.HGetAsync(_key, fieldBytes);
            
            if (valueBytes == null)
                return (false, default(TValue));

            TValue value = BsonConvert.DeserializeObject<TValue>(valueBytes);
            return (true, value);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
