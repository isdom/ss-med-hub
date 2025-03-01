package com.yulore.api;

import java.util.Map;

public interface MasterService {
    void updateHubStatus(final String ipAndPort, final Map<String, String> pathMapping, final long timestamp);
}
