using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Sets
{
    internal sealed class SPop : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly long? _count;

        public SPop(Stream stream, byte[] key)
            : this(stream, key, null)
        {
        }

        public SPop(Stream stream, byte[] key, long? count)
            : base(stream, "SPOP")
        {
            _key = key;
            _count = count;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);

            if (_count != null)
            {
                writer.WriteInteger(_count.Value);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            if (_count != null)
            {
                await writer.WriteIntegerAsync(_count.Value);
            }
        }
    }
}
