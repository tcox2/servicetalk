
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.PublisherSource.Subscription;

public class NullSubscription implements Subscription {

    @Override
    public void cancel() {
    }

    @Override
    public void request(long n) {
    }

}
