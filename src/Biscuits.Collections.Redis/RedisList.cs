using Biscuits.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Biscuits.Collections.Redis
{
    public class RedisList<T> : IList<T>, IReadOnlyList<T>
    {
        readonly RedisClient _client;
        readonly byte[] _key;
        readonly IEqualityComparer<T> _comparer;

        public RedisList(RedisClient client, byte[] key)
            : this(client, key, EqualityComparer<T>.Default)
        {
        }

        public RedisList(RedisClient client, byte[] key, IEqualityComparer<T> comparer)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _key = key ?? throw new ArgumentNullException(nameof(key));
            _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        }
        
        public T this[int index]
        {
            get { return Get(index); }
            set { Insert(index, value); }
        }

        public T Get(long index)
        {
            byte[] value;

            try
            {
                value = _client.LIndex(_key, index);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }

            return BsonConvert.DeserializeObject<T>(value);
        }

        public async Task<T> GetAsync(long index)
        {
            byte[] value;

            try
            {
                value = await _client.LIndexAsync(_key, index);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }

            return BsonConvert.DeserializeObject<T>(value);
        }

        public int Count
        {
            get{ return (int)GetCount(); }
        }

        public long GetCount()
        {
            return _client.LLen(_key);
        }

        public async Task<long> GetCountAsync()
        {
            return await _client.LLenAsync(_key);
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public void Add(T item)
        {
            byte[] value = BsonConvert.SerializeObject(item);
            _client.LPush(_key, new[] { value });
        }

        public async Task AddAsync(T item)
        {
            byte[] value = BsonConvert.SerializeObject(item);
            await _client.LPushAsync(_key, new[] { value });
        }

        public void AddRange(IEnumerable<T> items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var values = new List<byte[]>();

            foreach (T item in items)
            {
                byte[] value = BsonConvert.SerializeObject(item);
                values.Add(value);
            }

            _client.LPush(_key, values);
        }

        public async Task AddRangeAsync(IEnumerable<T> items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var values = new List<byte[]>();

            foreach (T item in items)
            {
                byte[] value = BsonConvert.SerializeObject(item);
                values.Add(value);
            }

            await _client.LPushAsync(_key, values);
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public async Task ClearAsync()
        {
            throw new NotImplementedException();
        }

        public bool Contains(T item)
        {
            return IndexOf(item) != 0;
        }

        public void CopyTo(T[] array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (index < 0 || index > array.Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (array.Length - index < Count)
                throw new ArgumentException("Destination array is not long enough.", nameof(array));

            foreach (T item in this)
            {
                array[index++] = item;
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _client.LRange(_key, 0, long.MaxValue).Select(BsonConvert.DeserializeObject<T>).GetEnumerator();
        }

        public int IndexOf(T item)
        {
            int index = 0;

            foreach (T element in this)
            {
                if (_comparer.Equals(element, item))
                    return index;

                index++;
            }

            return -1;
        }

        public void Insert(int index, T item)
        {
            try
            {
                byte[] value = BsonConvert.SerializeObject(item);
                _client.LSet(_key, index, value);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }
        }

        public async Task InsertAsync(int index, T item)
        {
            try
            {
                byte[] value = BsonConvert.SerializeObject(item);
                await _client.LSetAsync(_key, index, value);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }
        }

        public bool Remove(T item)
        {
            int index = IndexOf(item);

            if (index != -1)
            {
                RemoveAt(index);
                return true;
            }

            return false;
        }

        public async Task<bool> RemoveAsync(T item)
        {
            int index = IndexOf(item);

            if (index != -1)
            {
                await RemoveAtAsync(index);
                return true;
            }

            return false;
        }

        public void RemoveAt(int index)
        {
            byte[] random = BsonConvert.SerializeObject(Guid.NewGuid());

            try
            {
                _client.LSet(_key, index, random);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }

            _client.LRem(_key, 1, random);
        }

        public async Task RemoveAtAsync(int index)
        {
            byte[] random = BsonConvert.SerializeObject(Guid.NewGuid());

            try
            {
                await _client.LSetAsync(_key, index, random);
            }
            catch (RedisErrorException exception) when (string.Equals(exception.Message, "ERR index out of range", StringComparison.Ordinal))
            {
                throw new IndexOutOfRangeException();
            }

            await _client.LRemAsync(_key, 1, random);
        }

        public IList<T> GetAll()
        {
            IList<byte[]> values = _client.LRange(_key, 0, long.MaxValue);
            var items = new List<T>(values.Count);

            foreach (byte[] value in values)
            {
                T item = BsonConvert.DeserializeObject<T>(value);
                items.Add(item);
            }

            return items;
        }

        public async Task<IList<T>> GetAllAsync()
        {
            IList<byte[]> values = await _client.LRangeAsync(_key, 0, long.MaxValue);
            var items = new List<T>(values.Count);

            foreach (byte[] value in values)
            {
                T item = BsonConvert.DeserializeObject<T>(value);
                items.Add(item);
            }

            return items;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
