/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.dialogue.hc5;

import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.core5.function.Resolver;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

final class DialogueConnectionConfigResolver implements Resolver<HttpRoute, ConnectionConfig> {

    // Increased from two seconds to four seconds because we have strong support for retries
    // and can optimistically avoid expensive connection checks. Failures caused by NoHttpResponseExceptions
    // are possible when the target closes connections prior to this timeout, and can be safely retried.
    // Ideally this value would be larger for RPC, however some servers use relatively low defaults:
    // apache httpd versions 1.3 and 2.0: 15 seconds:
    // https://httpd.apache.org/docs/2.0/mod/core.html#keepalivetimeout
    // apache httpd version 2.2 and above: 5 seconds
    // https://httpd.apache.org/docs/2.2/mod/core.html#keepalivetimeout
    // nodejs http server: 5 seconds
    // https://nodejs.org/api/http.html#http_server_keepalivetimeout
    // nginx: 75 seconds (good)
    // https://nginx.org/en/docs/http/ngx_http_core_module.html#keepalive_timeout
    // dropwizard: 30 seconds (see idleTimeout in the linked docs)
    // https://www.dropwizard.io/en/latest/manual/configuration.html#Connectors
    // wc: 60 seconds (internal)
    private static final TimeValue CONNECTION_INACTIVITY_CHECK = TimeValue.ofMilliseconds(
            Integer.getInteger("dialogue.experimental.inactivity.check.threshold.millis", 4_000));

    /**
     * Upper bound on how long connections pooled before a stale-connection failure stay suspect when inactivity
     * validation is otherwise disabled. Pooled connections do not outlive the server's keep-alive timeout, so this
     * only needs to cover that.
     */
    private static final long MAX_SUSPECT_WINDOW_MILLIS = TimeUnit.MINUTES.toMillis(10);

    private final Timeout connectTimeout;
    private final Timeout socketTimeout;

    // We create a new connectionConfig when the connectionInactivityCheck interval changes
    // to avoid allocating a new ConnectionConfig each time the value is queried.
    private volatile ConnectionConfig connectionConfig;

    /**
     * Wall-clock time in milliseconds, the clock the pool uses for idle times, of the most recent failure showing
     * that connections pooled before it may be dead (see {@link #markPooledConnectionsSuspect()}), or 0 if none.
     */
    private volatile long pooledConnectionsSuspectBeforeMillis = 0;

    DialogueConnectionConfigResolver(Timeout connectTimeout, Timeout socketTimeout) {
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        setValidateAfterInactivity(CONNECTION_INACTIVITY_CHECK);
    }

    void setValidateAfterInactivity(TimeValue connectionInactivityCheck) {
        connectionConfig = ConnectionConfig.custom()
                .setValidateAfterInactivity(connectionInactivityCheck)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();
    }

    TimeValue getValidateAfterInactivity() {
        return connectionConfig.getValidateAfterInactivity();
    }

    /**
     * Called when a pooled connection turned out to be dead, e.g. because the server restarted. When that happens
     * every connection pooled to that server is usually dead too, but the pool only validates connections idle for
     * longer than {@link #getValidateAfterInactivity()}, which a server's {@code Keep-Alive} header can raise to
     * minutes. Until then, connections last used before this call are validated before they are reused, so a
     * request leases a live or new connection instead of the next dead one.
     */
    void markPooledConnectionsSuspect() {
        pooledConnectionsSuspectBeforeMillis = System.currentTimeMillis();
    }

    @Override
    public ConnectionConfig resolve(HttpRoute _ignored) {
        ConnectionConfig current = connectionConfig;
        long suspectBefore = pooledConnectionsSuspectBeforeMillis;
        if (suspectBefore == 0) {
            return current;
        }
        long sinceSuspectMillis = Math.max(0, System.currentTimeMillis() - suspectBefore);
        TimeValue validateAfterInactivity = current.getValidateAfterInactivity();
        long windowMillis = TimeValue.isNonNegative(validateAfterInactivity)
                ? validateAfterInactivity.toMilliseconds()
                : MAX_SUSPECT_WINDOW_MILLIS;
        if (sinceSuspectMillis >= windowMillis) {
            // Every connection last used before the failure has been idle long enough to be validated anyway.
            return current;
        }
        // The pool validates a connection whose last use is more than this long ago, i.e. before the failure.
        return ConnectionConfig.custom()
                .setValidateAfterInactivity(TimeValue.ofMilliseconds(sinceSuspectMillis))
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();
    }
}
