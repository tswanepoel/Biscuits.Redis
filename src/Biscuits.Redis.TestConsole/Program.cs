using System;
using System.Threading.Tasks;

namespace Biscuits.Redis.TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            RunAsync().Wait();
        }

        static async Task RunAsync()
        {
            using (var client = new RedisClient("localhost"))
            {
                string msg = await client.EchoAsync("Hello world!");
                Console.WriteLine(msg);

                string response = await client.SelectAsync(0);
                Console.WriteLine(response);

                long count = await client.RPushAsync("test", "one");
                Console.WriteLine(count);

                count = await client.RPushAsync("test", "two");
                Console.WriteLine(count);

                count = await client.LInsertBeforeAsync("test", "two", "one.five");
                Console.WriteLine("c: " + count);

                string val = await client.LIndexAsync("test", -2);
                Console.WriteLine(val);

                long len = await client.LLenAsync("test");
                Console.WriteLine("l: " + len);

                long len2 = await client.LPushAsync("test", "point.five");
                Console.WriteLine("l2: " + len2);

                long len3 = await client.LPushXAsync("test", "point.twofive");
                Console.WriteLine("l3: " + len3);

                long rem = await client.LRemAsync("test", -2, "one");
                Console.WriteLine("rem: " + rem);

                string s = await client.LSetAsync("test", -4, "upd");
                Console.WriteLine("s: " + s);

                foreach (string item in await client.LRangeAsync("test", 3, 5))
                {
                    Console.WriteLine("r: " + item);
                }

                string tr = await client.LTrimAsync("test", -2, 0);
                Console.WriteLine("tr: " + tr);

                long fin = await client.LPushAsync("test", "final");
                Console.WriteLine("fin: " + fin);

                string rot = await client.RPopLPushAsync("test", "test");
                Console.WriteLine("rot: " + rot);

                string value;
                
                while ((value = await client.LPopAsync("test")) != null)
                {
                    Console.WriteLine(value);
                }
            }

            Console.ReadLine();
        }
    }
}
