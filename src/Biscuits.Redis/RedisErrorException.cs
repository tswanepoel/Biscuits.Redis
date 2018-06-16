using System;

namespace Biscuits.Redis
{
    public class RedisErrorException : Exception
    {
        public RedisErrorException()
        {
        }

        public RedisErrorException(string message)
            : base(message)
        {
        }

        public RedisErrorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
