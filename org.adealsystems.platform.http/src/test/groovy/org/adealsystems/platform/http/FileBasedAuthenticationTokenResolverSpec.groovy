/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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
import org.apache.hc.client5.http.impl.classic.HttpClients
import spock.lang.Specification
import spock.lang.TempDir

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Flow
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

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
        1 * client.sendAsync(_ as HttpRequest, _ as HttpResponse.BodyHandler) >> { HttpRequest sentRequest, HttpResponse.BodyHandler ignored ->
            request = sentRequest
            CompletableFuture.completedFuture(response)
        }
        result == 'new-token'
        Files.readString(tokenFile) == 'new-token'
        request != null
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

    def 'requests and stores a token using Apache HttpClient 5'() {
        given:
        Path tokenFile = temporaryDirectory.resolve('apache-token')
        def requestBody = new AtomicReference<String>()
        def server = new ServerSocket(0)
        def serverThread = Thread.start {
            Socket socket = server.accept()
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.inputStream, StandardCharsets.UTF_8))
                reader.readLine()
                int contentLength = 0
                String header
                while ((header = reader.readLine()) != '') {
                    if (header.toLowerCase(Locale.ROOT).startsWith('content-length:')) {
                        contentLength = Integer.parseInt(header.substring(header.indexOf(':') + 1).trim())
                    }
                }
                char[] body = new char[contentLength]
                reader.read(body)
                requestBody.set(new String(body))

                byte[] responseBody = '{"access_token":"apache-token"}'.getBytes(StandardCharsets.UTF_8)
                String responseHeaders = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${responseBody.length}\r\nConnection: close\r\n\r\n"
                socket.outputStream.write(responseHeaders.getBytes(StandardCharsets.UTF_8))
                socket.outputStream.write(responseBody)
                socket.outputStream.flush()
            }
            finally {
                socket.close()
            }
        }
        def client = HttpClients.createDefault()
        def resolver = resolverFor(tokenFile, "http://localhost:${server.localPort}/token")

        try {
            when:
            def result = resolver.resolveToken(client)

            then:
            result == 'apache-token'
            Files.readString(tokenFile) == 'apache-token'
            requestBody.get() == 'client_id=gateway&grant_type=password&username=user%40example.com&password=p%40ss'
        }
        finally {
            client.close()
            server.close()
            serverThread.join(1000)
        }
    }

    private static FileBasedAuthenticationTokenResolver resolverFor(Path tokenFile) {
        resolverFor(tokenFile, 'https://auth.example/token')
    }

    private static FileBasedAuthenticationTokenResolver resolverFor(Path tokenFile, String authServiceUrl) {
        new FileBasedAuthenticationTokenResolver(
            tokenFile.toString(),
            authServiceUrl,
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
