/*
 * Copyright © 2021 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.internal.DeliberateException;
import io.servicetalk.concurrent.test.internal.TestCompletableSubscriber;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.servicetalk.concurrent.api.Completable.completed;
import static io.servicetalk.concurrent.api.Completable.failed;
import static io.servicetalk.concurrent.api.SourceAdapters.toSource;
import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class OnErrorCompletableTest {
    private TestCompletableSubscriber subscriber;
    private TestCompletable first;

    @BeforeEach
    public void setUp() {
        subscriber = new TestCompletableSubscriber();
        first = new TestCompletable();
    }

    @Test
    public void onErrorComplete() {
        toSource(first.onErrorComplete()).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        subscriber.awaitOnComplete();
    }

    @Test
    public void onErrorCompleteClassMatch() {
        toSource(first.onErrorComplete(DeliberateException.class)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        subscriber.awaitOnComplete();
    }

    @Test
    public void onErrorCompleteClassNoMatch() {
        toSource(first.onErrorComplete(IllegalArgumentException.class)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorCompletePredicateMatch() {
        toSource(first.onErrorComplete(t -> t == DELIBERATE_EXCEPTION)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        subscriber.awaitOnComplete();
    }

    @Test
    public void onErrorCompletePredicateNoMatch() {
        toSource(first.onErrorComplete(t -> false)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapMatch() {
        toSource(first.onErrorMap(t -> DELIBERATE_EXCEPTION)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapMatchThrows() {
        toSource(first.onErrorMap(t -> {
            throw DELIBERATE_EXCEPTION;
        })).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapClassMatch() {
        toSource(first.onErrorMap(DeliberateException.class, t -> DELIBERATE_EXCEPTION)).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapClassNoMatch() {
        toSource(first.onErrorMap(IllegalArgumentException.class, t -> new DeliberateException()))
                .subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapPredicateMatch() {
        toSource(first.onErrorMap(t -> t instanceof DeliberateException, t -> DELIBERATE_EXCEPTION))
                .subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorMapPredicateNoMatch() {
        toSource(first.onErrorMap(t -> false, t -> new IllegalStateException())).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorResumeClassMatch() {
        toSource(first.onErrorResume(DeliberateException.class, t -> failed(DELIBERATE_EXCEPTION)))
                .subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorResumeClassNoMatch() {
        toSource(first.onErrorResume(IllegalArgumentException.class, t -> completed())).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorResumePredicateMatch() {
        toSource(first.onErrorResume(t -> t instanceof DeliberateException,
                t -> failed(DELIBERATE_EXCEPTION))).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(new DeliberateException());
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }

    @Test
    public void onErrorResumePredicateNoMatch() {
        toSource(first.onErrorResume(t -> false, t -> completed())).subscribe(subscriber);
        subscriber.awaitSubscription();
        first.onError(DELIBERATE_EXCEPTION);
        assertThat(subscriber.awaitOnError(), is(DELIBERATE_EXCEPTION));
    }
}
