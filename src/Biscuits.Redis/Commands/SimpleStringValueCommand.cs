using Biscuits.Redis.Resp;
using System.IO;

namespace Biscuits.Redis.Commands
{
    internal abstract class SimpleStringValueCommand : Command<string>
    {
        protected SimpleStringValueCommand(Stream stream, string name)
            : base(stream, name)
        {
        }
        
        protected override CommandResult<string> ReadResult(IRespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                return CommandResult<string>.Error(err);
            }

            if (dataType != RespDataType.SimpleString)
            {
                throw new InvalidDataException();
            }

            string value = reader.ReadSimpleStringValue();
            return CommandResult.Success(value);
        }
    }
}
