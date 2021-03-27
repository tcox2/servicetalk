
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.PublisherSource.Subscription;

import javax.annotation.Nullable;

public class NullSubscriber<T> implements Subscriber<T> {

    @Override
    public void onSubscribe(Subscription subscription) {
    }

    @Override
    public void onNext(@Nullable Object o) {
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onComplete() {
    }

}
