/*
 * Copyright © 2018 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.concurrent.api.single;

import io.servicetalk.concurrent.api.Single;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Consumer;

import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;

public class DoAfterSuccessTest extends AbstractDoSuccessTest {
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Override
    protected <T> Single<T> doSuccess(Single<T> single, Consumer<T> consumer) {
        return single.doAfterSuccess(consumer);
    }

    @Test
    @Override
    public void testCallbackThrowsError() {
        listener.listen(doSuccess(Single.success("Hello"), t -> {
            throw DELIBERATE_EXCEPTION;
        })).verifySuccess("Hello");
    }
}
