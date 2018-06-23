using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Lists
{
    internal sealed class RPushX : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _value;

        public RPushX(Stream stream, byte[] key, byte[] value)
            : base(stream, "RPUSHX")
        {
            _key = key;
            _value = value;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
