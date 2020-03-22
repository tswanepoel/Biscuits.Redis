using System;
using System.Runtime.Serialization;

namespace Biscuits.Redis
{
    [Serializable]
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

        protected RedisErrorException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
