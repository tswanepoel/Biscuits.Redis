using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Lists
{
    internal sealed class LInsertAfter : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _after;
        readonly byte[] _value;

        public LInsertAfter(Stream stream, byte[] key, byte[] after, byte[] value)
            : base(stream, "LINSERT")
        {
            _key = key;
            _after = after;
            _value = value;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString("AFTER");
            writer.WriteBulkString(_after);
            writer.WriteBulkString(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync("AFTER");
            await writer.WriteBulkStringAsync(_after);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
