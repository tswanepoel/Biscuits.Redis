using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;

namespace Biscuits.Redis
{
    internal abstract class ArrayOfBulkStringValueCommand : Command<IList<byte[]>>
    {
        protected ArrayOfBulkStringValueCommand(Stream stream, string name)
            : base(stream, name)
        {
        }
        
        protected override CommandResult<IList<byte[]>> ReadResult(IRespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                return CommandResult<IList<byte[]>>.Error(err);
            }

            if (dataType != RespDataType.Array)
                throw new InvalidDataException();

            if (!reader.TryReadStartArray(out long length))
                return CommandResult<IList<byte[]>>.Success(null);

            var values = new List<byte[]>();

            for (int i = 0; i < length; i++)
            {
                dataType = reader.ReadDataType();

                if (dataType != RespDataType.BulkString)
                    throw new InvalidDataException();

                byte[] value = reader.ReadBulkStringValue();
                values.Add(value);
            }

            return CommandResult.Success((IList<byte[]>)values);
        }
    }
}
