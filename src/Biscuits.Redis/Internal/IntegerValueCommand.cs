using Biscuits.Redis.Resp;
using System.IO;

namespace Biscuits.Redis
{
    internal abstract class IntegerValueCommand : Command<long>
    {
        protected IntegerValueCommand(Stream stream, string name)
            : base(stream, name)
        {
        }
        
        protected override CommandResult<long> ReadResult(IRespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                return CommandResult<long>.Error(err);
            }

            if (dataType != RespDataType.Integer)
                throw new InvalidDataException();

            long value = reader.ReadIntegerValue();
            return CommandResult.Success(value);
        }
    }
}
