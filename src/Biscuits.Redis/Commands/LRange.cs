using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class LRange : ArrayOfBulkStringValueCommand
    {
        readonly byte[] _key;
        readonly long _start;
        readonly long _stop;

        public LRange(Stream stream, byte[] key, long start, long stop)
            : base(stream, "LRANGE")
        {
            _key = key;
            _start = start;
            _stop = stop;
        }

        protected override async Task WriteParametersAsync(IRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_start.ToString(CultureInfo.InvariantCulture));
            await writer.WriteBulkStringAsync(_stop.ToString(CultureInfo.InvariantCulture));
        }
    }
}
