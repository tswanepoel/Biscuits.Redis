namespace Biscuits.Redis.Commands
{
    internal static class CommandResult
    {
        public static CommandResult<T> Success<T>(T result)
        {
            return CommandResult<T>.Success(result);
        }
    }
}
