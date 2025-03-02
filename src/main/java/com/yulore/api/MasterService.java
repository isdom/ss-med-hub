package com.yulore.api;

import java.util.List;
import java.util.Map;

public interface MasterService {
    void updateHubStatus(final String ipAndPort, final Map<String, String> pathMapping, final long timestamp);
    List<String> getUrlsOf(final String handler);
}
