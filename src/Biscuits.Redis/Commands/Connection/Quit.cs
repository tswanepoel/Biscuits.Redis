using System.IO;

namespace Biscuits.Redis.Commands.Connection
{
    internal sealed class Quit : SimpleStringValueCommand
    {
        public Quit(Stream stream)
            : base(stream, "QUIT")
        {
        }
    }
}
