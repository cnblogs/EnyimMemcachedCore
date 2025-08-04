using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace Enyim.Caching.Configuration;
public static class EndPointExtensions
{
    public static IPEndPoint GetIPEndPoint(this EndPoint endpoint, bool useIPv6)
    {
        if (endpoint is IPEndPoint ipEndPoint)
        {
            return ipEndPoint;
        }
        else if (endpoint is DnsEndPoint dnsEndPoint)
        {
            var address = Dns.GetHostAddresses(dnsEndPoint.Host).FirstOrDefault(ip =>
                ip.AddressFamily == (useIPv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork));
            return address == null
                ? throw new ArgumentException(string.Format("Could not resolve host '{0}'.", endpoint))
                : new IPEndPoint(address, dnsEndPoint.Port);
        }
        else
        {
            throw new Exception("Not supported EndPoint type");
        }
    }
}
