package com.yulore.util;

import lombok.extern.slf4j.Slf4j;

import java.net.*;
import java.util.Enumeration;

@Slf4j
public class NetworkUtil {
    public static Inet4Address getLocalIpv4() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                final NetworkInterface iface = interfaces.nextElement();
                // 过滤回环接口、虚拟接口和非活动接口
                if (iface.isLoopback() || iface.isVirtual() || !iface.isUp()) {
                    continue;
                }

                final Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    final InetAddress addr = addresses.nextElement();
                    // 优先返回 IPv4 地址
                    if (addr instanceof Inet4Address ipv4) {
                        log.info("interface name: {}", iface.getDisplayName());
                        log.info("ipv4 address: {}", ipv4.getHostAddress());
                        return ipv4;
                    }
                }
            }
        } catch (SocketException ex) {
            log.warn("getRealIp: failed, detail: {}", ExceptionUtil.exception2detail(ex));
        }
        return null;
    }

    public static String getLocalIpv4AsString() {
        final Inet4Address localAddress = getLocalIpv4();
        return localAddress != null ? localAddress.getHostAddress() : "(null)";
    }

    // 安全获取主机名，避免重复调用
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Failed to get hostname, fallback to 'unknown'", e);
            return "unknown";
        }
    }
}
