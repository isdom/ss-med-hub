package com.yulore.medhub.ws;

import com.yulore.api.MasterService;
import org.redisson.api.RFuture;
import org.redisson.api.annotation.RRemoteAsync;

import java.util.List;
import java.util.Map;

@RRemoteAsync(MasterService.class)
public interface MasterServiceAsync {
    RFuture<Void> updateHubStatus(final String ipAndPort,
                                  final Map<String, String> pathMapping,
                                  final long timestamp);
    RFuture<List<String>> getUrlsOf(final String handler);
}
