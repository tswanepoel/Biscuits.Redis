using Biscuits.Redis.Resp;
using System.IO;

namespace Biscuits.Redis
{
    internal abstract class BulkStringValueCommand : Command<byte[]>
    {
        protected BulkStringValueCommand(Stream stream, string name)
            : base(stream, name)
        {
        }
        
        protected override CommandResult<byte[]> ReadResult(IRespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                return CommandResult<byte[]>.Error(err);
            }

            if (dataType != RespDataType.BulkString)
                throw new InvalidDataException();

            byte[] value = reader.ReadBulkStringValue();
            return CommandResult.Success(value);
        }
    }
}
