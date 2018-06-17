namespace Biscuits.Redis
{
    internal static class CommandResult
    {
        public static CommandResult<T> Success<T>(T result)
        {
            return CommandResult<T>.Success(result);
        }
    }

    internal class CommandResult<T>
    {
        private readonly string _errorMessage;
        private readonly T _result;
        
        private CommandResult(string errorMessage)
        {
            _errorMessage = errorMessage;
            IsError = true;
        }

        private CommandResult(T result)
        {
            _result = result;
        }

        public bool IsError { get; }

        public T Result
        {
            get
            {
                if (IsError)
                {
                    throw new RedisErrorException(_errorMessage);
                }

                return _result;
            }
        }

        public static CommandResult<T> Success(T result)
        {
            return new CommandResult<T>(result);
        }

        public static CommandResult<T> Error(string message)
        {
            return new CommandResult<T>(message);
        }
    }
}
