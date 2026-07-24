/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

package org.adealsystems.platform.http

import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification
import spock.lang.TempDir

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.Flow

class FileBasedAuthenticationTokenResolverSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @TempDir
    Path temporaryDirectory

    def 'uses an existing token without making an HTTP request'() {
        given:
        Path tokenFile = temporaryDirectory.resolve('token')
        Files.writeString(tokenFile, 'stored-token\n')
        def client = Mock(HttpClient)
        def resolver = resolverFor(tokenFile)

        when:
        def result = resolver.resolveToken(client)

        then:
        result == 'stored-token'
        0 * client._
    }

    def 'requests and stores a token using the expected form payload'() {
        given:
        Path tokenFile = temporaryDirectory.resolve('token')
        def client = Mock(HttpClient)
        def response = Mock(HttpResponse)
        HttpRequest request
        response.body() >> '{"access_token":"new-token"}'
        def resolver = resolverFor(tokenFile)

        when:
        def result = resolver.resolveToken(client)

        then:
        1 * client.send(_ as HttpRequest, _ as HttpResponse.BodyHandler) >> { HttpRequest sentRequest, HttpResponse.BodyHandler ignored ->
            request = sentRequest
            response
        }
        result == 'new-token'
        Files.readString(tokenFile) == 'new-token'
        request.method() == 'POST'
        request.uri().toString() == 'https://auth.example/token'
        request.headers().firstValue('Content-Type').get() == 'application/x-www-form-urlencoded'
        requestBody(request) == 'client_id=gateway&grant_type=password&username=user%40example.com&password=p%40ss'
    }

    def 'uses a token written by another resolver while refreshing'() {
        given:
        Path tokenFile = temporaryDirectory.resolve('token')
        Files.writeString(tokenFile, 'token-from-another-resolver')
        def client = Mock(HttpClient)
        def resolver = resolverFor(tokenFile)

        when:
        def result = resolver.refreshToken(client, 'rejected-token')

        then:
        result == 'token-from-another-resolver'
        0 * client._
    }

    private FileBasedAuthenticationTokenResolver resolverFor(Path tokenFile) {
        new FileBasedAuthenticationTokenResolver(
            tokenFile.toString(),
            'https://auth.example/token',
            'client_id=gateway&grant_type=password&username=${username}&password=${password}',
            'user@example.com',
            'p@ss',
            OBJECT_MAPPER
        )
    }

    private static String requestBody(HttpRequest request) {
        def output = new ByteArrayOutputStream()
        def completed = new CountDownLatch(1)
        request.bodyPublisher().get().subscribe(new Flow.Subscriber<ByteBuffer>() {
            @Override
            void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE)
            }

            @Override
            void onNext(ByteBuffer buffer) {
                byte[] bytes = new byte[buffer.remaining()]
                buffer.get(bytes)
                output.write(bytes)
            }

            @Override
            void onError(Throwable throwable) {
                completed.countDown()
            }

            @Override
            void onComplete() {
                completed.countDown()
            }
        })
        assert completed.await(1, TimeUnit.SECONDS)
        output.toString(StandardCharsets.UTF_8.name())
    }
}
