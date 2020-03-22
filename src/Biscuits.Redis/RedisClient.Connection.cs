using Biscuits.Redis.Commands.Connection;
using System;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    public partial class RedisClient
    {
        #region Auth

        public string Auth(string password)
        {
            ValidateNotDisposed();

            if (password == null)
            {
                throw new ArgumentNullException(nameof(password));
            }

            byte[] bytes = _encoding.GetBytes(password);
            return _encoding.GetString(Echo(bytes));
        }

        public string Auth(byte[] password)
        {
            ValidateNotDisposed();

            if (password == null)
            {
                throw new ArgumentNullException(nameof(password));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Auth(connection.GetStream(), password);

            return command.Execute();
        }

        public async Task<string> AuthAsync(string password)
        {
            ValidateNotDisposed();

            if (password == null)
            {
                throw new ArgumentNullException(nameof(password));
            }

            byte[] bytes = _encoding.GetBytes(password);
            return await AuthAsync(bytes);
        }

        public async Task<string> AuthAsync(byte[] password)
        {
            ValidateNotDisposed();

            if (password == null)
            {
                throw new ArgumentNullException(nameof(password));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Auth(connection.GetStream(), password);

            return await command.ExecuteAsync();
        }

        #endregion

        #region Echo

        public string Echo(string message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(Echo(bytes));
        }

        public byte[] Echo(byte[] message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Echo(connection.GetStream(), message);

            return command.Execute();
        }

        public async Task<string> EchoAsync(string message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            byte[] bytes = _encoding.GetBytes(message);
            return _encoding.GetString(await EchoAsync(bytes));
        }

        public async Task<byte[]> EchoAsync(byte[] message)
        {
            ValidateNotDisposed();

            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Echo(connection.GetStream(), message);

            return await command.ExecuteAsync();
        }

        #endregion

        #region Ping

        public string Ping()
        {
            return Ping((byte[])null);
        }

        public string Ping(string message)
        {
            ValidateNotDisposed();

            byte[] bytes = _encoding.GetBytes(message);
            return Ping(bytes);
        }

        public string Ping(byte[] message)
        {
            ValidateNotDisposed();

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Ping(connection.GetStream(), message);

            return command.Execute();
        }

        public async Task<string> PingAsync()
        {
            return await PingAsync((byte[])null);
        }

        public async Task<string> PingAsync(string message)
        {
            ValidateNotDisposed();

            byte[] bytes = _encoding.GetBytes(message);
            return await PingAsync(bytes);
        }

        public async Task<string> PingAsync(byte[] message)
        {
            ValidateNotDisposed();

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Ping(connection.GetStream(), message);

            return await command.ExecuteAsync();
        }

        #endregion

        #region Quit
        
        public string Quit()
        {
            ValidateNotDisposed();

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Quit(connection.GetStream());

            return command.Execute();
        }
        
        public async Task<string> QuitAsync()
        {
            ValidateNotDisposed();

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Quit(connection.GetStream());

            return await command.ExecuteAsync();
        }

        #endregion

        #region Select

        public string Select(int index)
        {
            ValidateNotDisposed();

            if (index < 0 || index > 15)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Select(connection.GetStream(), index);

            return command.Execute();
        }

        public async Task<string> SelectAsync(int index)
        {
            ValidateNotDisposed();

            if (index < 0 || index > 15)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            using var connection = new RedisConnection(_connectionSettings);
            var command = new Select(connection.GetStream(), index);

            return await command.ExecuteAsync();
        }

        #endregion
    }
}
