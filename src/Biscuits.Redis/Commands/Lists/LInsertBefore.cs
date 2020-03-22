using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Lists
{
    internal sealed class LInsertBefore : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly byte[] _before;
        private readonly byte[] _value;

        public LInsertBefore(Stream stream, byte[] key, byte[] before, byte[] value)
            : base(stream, "LINSERT")
        {
            _key = key;
            _before = before;
            _value = value;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString("BEFORE");
            writer.WriteBulkString(_before);
            writer.WriteBulkString(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync("BEFORE");
            await writer.WriteBulkStringAsync(_before);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
