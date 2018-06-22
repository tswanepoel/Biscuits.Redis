using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class RPop : BulkStringValueCommand
    {
        readonly byte[] _key;

        public RPop(Stream stream, byte[] key)
            : base(stream, "RPOP")
        {
            _key = key;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
        }
    }
}
