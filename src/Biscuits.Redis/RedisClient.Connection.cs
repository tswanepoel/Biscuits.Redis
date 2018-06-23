using Biscuits.Redis.Commands;
using System;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region Select

        public string Select(int index)
        {
            ValidateNotDisposed();

            if (index < 0 || index > 15)
                throw new ArgumentOutOfRangeException(nameof(index));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new Select(connection.GetStream(), index);
                return command.Execute();
            }
        }

        public async Task<string> SelectAsync(int index)
        {
            ValidateNotDisposed();

            if (index < 0 || index > 15)
                throw new ArgumentOutOfRangeException(nameof(index));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new Select(connection.GetStream(), index);
                return await command.ExecuteAsync();
            }
        }

        #endregion

        #region Echo

        public string Echo(string message)
        {
            ValidateNotDisposed();

            if (message == null)
                throw new ArgumentNullException(nameof(message));

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(Echo(bytes));
        }

        public byte[] Echo(byte[] message)
        {
            ValidateNotDisposed();

            if (message == null)
                throw new ArgumentNullException(nameof(message));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new Echo(connection.GetStream(), message);
                return command.Execute();
            }
        }

        public async Task<string> EchoAsync(string message)
        {
            ValidateNotDisposed();

            if (message == null)
                throw new ArgumentNullException(nameof(message));

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(await EchoAsync(bytes));
        }

        public async Task<byte[]> EchoAsync(byte[] message)
        {
            ValidateNotDisposed();

            if (message == null)
                throw new ArgumentNullException(nameof(message));

            using (var connection = new RedisConnection(_connectionSettings))
            {
                var command = new Echo(connection.GetStream(), message);
                return await command.ExecuteAsync();
            }
        }

        #endregion
    }
}
