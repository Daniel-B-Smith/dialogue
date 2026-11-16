/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.Request;
import com.palantir.dialogue.RequestBody;
import com.palantir.dialogue.Response;
import com.palantir.dialogue.TestConfigurations;
import com.palantir.dialogue.TestEndpoint;
import com.palantir.dialogue.core.DialogueChannel;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.util.HttpString;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Clients must not fail requests on dead pooled connections after a server restarts.
 * <p>
 * When a server advertises {@code Keep-Alive: timeout=N} (Witchcraft sends {@code timeout=180}),
 * {@link InactivityValidationAwareConnectionKeepAliveStrategy} raises the pool's validate-after-inactivity
 * threshold to N, so pooled connections idle for less than N are reused without a stale check. That holds while
 * the server process that accepted them is alive, but when it restarts every pooled connection to it is dead,
 * and each one fails the next request that leases it. Retries lease connections from the same pool (LIFO), so
 * they can draw dead connections too: before the fix, a non-retrying client failed one request per pooled
 * connection (20 of 30), and a client with 4 retries failed 4 of 30 requests outright.
 * <p>
 * Each test grows the pool to {@link #POOLED_CONNECTIONS} connections, restarts the server on the same port,
 * waits past the default 4 s inactivity threshold but well inside the advertised keep-alive timeout, then sends
 * requests one at a time. The assertions state the desired behaviour: a server restart should cost at most one
 * failed attempt, not one per pooled connection.
 */
public final class StalePooledConnectionsAfterServerRestartTest {

    private static final int POOLED_CONNECTIONS = 20;
    private static final int REQUESTS_AFTER_RESTART = POOLED_CONNECTIONS + 10;
    // Longer than the default validate-after-inactivity threshold (4 s), shorter than the advertised keep-alive.
    private static final Duration IDLE_BEFORE_REUSE = Duration.ofSeconds(5);
    private static final Request REQUEST = Request.builder().build();

    @Test
    void nonRetryingClientRecoversFromDeadPooledConnections() throws Exception {
        Result result = run(true, 0, REQUEST);
        assertThat(result.failures)
                .as("non-retrying client: requests failing after the restart (%s)", result)
                .isZero();
    }

    @Test
    void retryingClientRecoversFromDeadPooledConnections() throws Exception {
        Result result = run(true, 4, REQUEST);
        assertThat(result.failures)
                .as("retrying client (4 retries): requests failing after the restart (%s)", result)
                .isZero();
    }

    /**
     * A request that cannot be resent fails on the first dead connection, but must not leave the rest of the dead
     * connections for later requests.
     */
    @Test
    void nonRepeatableRequestFailsOnceThenPoolRecovers() throws Exception {
        Request nonRepeatable = Request.builder().body(new NonRepeatableBody()).build();
        Result result = run(true, 0, nonRepeatable);
        assertThat(result.failures)
                .as("non-repeatable requests failing after the restart (%s)", result)
                .isOne();
    }

    /** Control: without the Keep-Alive header, the default 4 s validation catches the dead connections. */
    @Test
    void withoutKeepAliveHeaderDeadConnectionsAreValidatedAway() throws Exception {
        Result result = run(false, 0, REQUEST);
        assertThat(result.failures)
                .as("non-retrying client, no Keep-Alive header (%s)", result)
                .isZero();
    }

    private static Result run(boolean advertiseKeepAlive, int maxNumRetries, Request request) throws Exception {
        CountDownLatch allArrived = new CountDownLatch(POOLED_CONNECTIONS);
        Undertow first = startServer(0, advertiseKeepAlive, allArrived);
        int port = getPort(first);
        String uri = "https://localhost:" + port;
        ClientConfiguration conf = ClientConfiguration.builder()
                .from(TestConfigurations.create(uri))
                .maxNumRetries(maxNumRetries)
                .backoffSlotSize(Duration.ofMillis(10))
                .build();
        Undertow second = null;
        try (ApacheHttpClientChannels.CloseableClient client =
                ApacheHttpClientChannels.createCloseableHttpClient(conf, "stale-pool-test")) {
            // Raw channel: no concurrency limiter, so the pool really grows to POOLED_CONNECTIONS connections.
            Channel raw = ApacheHttpClientChannels.createSingleUri(uri, client);
            List<ListenableFuture<Response>> warmup = new ArrayList<>();
            for (int i = 0; i < POOLED_CONNECTIONS; i++) {
                warmup.add(raw.execute(TestEndpoint.POST, REQUEST));
            }
            for (ListenableFuture<Response> future : warmup) {
                try (Response response = future.get(30, TimeUnit.SECONDS)) {
                    assertThat(response.code()).isEqualTo(200);
                }
            }

            // "Restart": every connection pooled so far belonged to the first server process.
            first.stop();
            second = startServer(port, advertiseKeepAlive, new CountDownLatch(0));
            Uninterruptibles.sleepUninterruptibly(IDLE_BEFORE_REUSE);

            Channel channel = DialogueChannel.builder()
                    .channelName("stale-pool-test")
                    .clientConfiguration(conf)
                    .factory(args -> ApacheHttpClientChannels.createSingleUri(args, client))
                    .build();
            int failures = 0;
            Optional<Throwable> firstFailure = Optional.empty();
            long start = System.nanoTime();
            for (int i = 0; i < REQUESTS_AFTER_RESTART; i++) {
                try (Response response =
                        channel.execute(TestEndpoint.POST, request).get(30, TimeUnit.SECONDS)) {
                    assertThat(response.code()).isEqualTo(200);
                } catch (ExecutionException e) {
                    failures++;
                    if (firstFailure.isEmpty()) {
                        firstFailure = Optional.of(e.getCause());
                    }
                }
            }
            return new Result(
                    failures, Duration.ofNanos(System.nanoTime() - start), firstFailure.map(Throwable::toString));
        } finally {
            first.stop();
            if (second != null) {
                second.stop();
            }
        }
    }

    private record Result(int failures, Duration elapsed, Optional<String> firstFailure) {
        @Override
        public String toString() {
            return failures + " of " + REQUESTS_AFTER_RESTART + " requests failed in " + elapsed.toMillis()
                    + " ms; first failure: " + firstFailure.orElse("none");
        }
    }

    private static Undertow startServer(int port, boolean advertiseKeepAlive, CountDownLatch holdUntilAllArrived) {
        HttpHandler handler = new BlockingHandler(exchange -> {
            // Hold the first requests until all have arrived, so each one gets its own connection.
            holdUntilAllArrived.countDown();
            holdUntilAllArrived.await(30, TimeUnit.SECONDS);
            if (advertiseKeepAlive) {
                exchange.getResponseHeaders().put(HttpString.tryFromString("Keep-Alive"), "timeout=180");
            }
            exchange.setStatusCode(200);
        });
        Undertow server = Undertow.builder()
                .setHandler(handler)
                .addHttpsListener(port, "localhost", SslSocketFactories.createSslContext(TestConfigurations.SSL_CONFIG))
                .build();
        server.start();
        return server;
    }

    /** A small body that can only be written once, so a failed attempt must not be resent. */
    private static final class NonRepeatableBody implements RequestBody {
        @Override
        public void writeTo(OutputStream output) throws IOException {
            output.write(new byte[] {1, 2, 3});
        }

        @Override
        public String contentType() {
            return "application/octet-stream";
        }

        @Override
        public boolean repeatable() {
            return false;
        }

        @Override
        public void close() {}
    }

    private static int getPort(Undertow undertow) {
        return ((InetSocketAddress)
                        Iterables.getOnlyElement(undertow.getListenerInfo()).getAddress())
                .getPort();
    }
}
