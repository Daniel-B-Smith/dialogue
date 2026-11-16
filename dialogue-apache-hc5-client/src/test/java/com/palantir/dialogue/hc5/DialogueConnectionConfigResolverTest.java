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

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.junit.jupiter.api.Test;

final class DialogueConnectionConfigResolverTest {

    private final DialogueConnectionConfigResolver resolver =
            new DialogueConnectionConfigResolver(Timeout.ofSeconds(1), Timeout.ofSeconds(2));

    @Test
    void usesTheConfiguredValidationIntervalUntilConnectionsAreSuspect() {
        resolver.setValidateAfterInactivity(TimeValue.ofSeconds(180));

        assertThat(resolver.resolve(null).getValidateAfterInactivity()).isEqualTo(TimeValue.ofSeconds(180));
    }

    @Test
    void validatesConnectionsLastUsedBeforeTheyBecameSuspect() {
        resolver.setValidateAfterInactivity(TimeValue.ofSeconds(180));

        resolver.markPooledConnectionsSuspect();
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(50));

        // Connections idle for longer than the time since the failure were last used before it.
        TimeValue validateAfter = resolver.resolve(null).getValidateAfterInactivity();
        assertThat(validateAfter.toMilliseconds()).isBetween(50L, 10_000L);
        assertThat(resolver.resolve(null).getConnectTimeout()).isEqualTo(Timeout.ofSeconds(1));
        assertThat(resolver.resolve(null).getSocketTimeout()).isEqualTo(Timeout.ofSeconds(2));
    }

    @Test
    void returnsToTheConfiguredIntervalOnceSuspectConnectionsAreOldEnough() {
        resolver.setValidateAfterInactivity(TimeValue.ofMilliseconds(20));

        resolver.markPooledConnectionsSuspect();
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(50));

        assertThat(resolver.resolve(null).getValidateAfterInactivity()).isEqualTo(TimeValue.ofMilliseconds(20));
    }
}
