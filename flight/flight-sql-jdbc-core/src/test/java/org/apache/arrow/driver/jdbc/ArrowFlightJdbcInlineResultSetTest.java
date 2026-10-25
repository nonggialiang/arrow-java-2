/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link ArrowFlightJdbcInlineResultSet}: batch consumption, responseId capture from
 * app_metadata, ticket replay with {@code x-fg-resume-response-id} after mid-stream failures, and
 * the bounded resume budget. {@link FlightStream}s are scripted Mockito mocks.
 *
 * <p>The last two tests exercise the factory branches against a real (mock-producer) server: the
 * inline+scroll mutual exclusion and the fallback to the regular stream result set when the server
 * does not return an INLINE ticket.
 */
public class ArrowFlightJdbcInlineResultSetTest {

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  private static final String INLINE_TICKET =
      "INLINE|bucket|prefix|query-1|user|123456|-|c0ffee";

  private RootAllocator allocator;
  private Schema schema;
  private final List<VectorSchemaRoot> rootsToClose = new ArrayList<>();
  private final List<ArrowBuf> buffersToClose = new ArrayList<>();

  /** The ResultSet under test, with {@code openStream} replaced by a scripted queue. */
  private final class ScriptedResultSet extends ArrowFlightJdbcInlineResultSet {
    private final Queue<FlightStream> pendingStreams;
    private final List<String> resumeRequests = new ArrayList<>();

    ScriptedResultSet(FlightStream... streams) throws SQLException {
      super(
          null,
          new QueryState(),
          testSignature(),
          new AvaticaResultSetMetaData(null, null, testSignature()),
          TimeZone.getDefault(),
          null,
          inlineInfo());
      this.pendingStreams = new ArrayDeque<>(Arrays.asList(streams));
    }

    @Override
    protected FlightStream openStream(final String resumeResponseId) {
      if (resumeResponseId != null) {
        resumeRequests.add(resumeResponseId);
      }
      return pendingStreams.remove();
    }
  }

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    schema = new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));
  }

  @AfterEach
  public void tearDown() {
    rootsToClose.forEach(VectorSchemaRoot::close);
    rootsToClose.clear();
    buffersToClose.forEach(ArrowBuf::close);
    buffersToClose.clear();
    allocator.close();
  }

  @Test
  public void shouldConsumeAllBatchesAndCaptureResponseIds() throws Exception {
    final VectorSchemaRoot root = root();
    final FlightStream stream = streamOf(root);
    // one stream, two batches, then clean completion
    when(stream.next())
        .thenAnswer(
            inv -> {
              fill(root, 1, 2, 3);
              return true;
            })
        .thenAnswer(
            inv -> {
              fill(root, 4, 5);
              return true;
            })
        .thenReturn(false);
    when(stream.getLatestMetadata()).thenReturn(metaBuf("resp-1")).thenReturn(metaBuf("resp-2"));

    try (ScriptedResultSet resultSet = new ScriptedResultSet(stream)) {
      resultSet.execute();
      assertEquals(Arrays.asList(1, 2, 3, 4, 5), readAll(resultSet));
      // stays exhausted
      assertFalse(resultSet.next());
      assertEquals("resp-2", resultSet.getLastResponseId());
      assertTrue(resultSet.resumeRequests.isEmpty());
      assertEquals(0, resultSet.getResumeAttempts());
    }
  }

  @Test
  public void shouldResumeAfterMidStreamFailure() throws Exception {
    final FlightRuntimeException broken =
        CallStatus.UNAVAILABLE.withDescription("stream broken").toRuntimeException();

    final VectorSchemaRoot root1 = root();
    final FlightStream first = streamOf(root1);
    when(first.next())
        .thenAnswer(
            inv -> {
              fill(root1, 1, 2, 3);
              return true;
            })
        .thenThrow(broken);
    when(first.getLatestMetadata()).thenReturn(metaBuf("resp-1"));

    final VectorSchemaRoot root2 = root();
    final FlightStream second = streamOf(root2);
    when(second.next())
        .thenAnswer(
            inv -> {
              fill(root2, 4, 5, 6);
              return true;
            })
        .thenReturn(false);
    when(second.getLatestMetadata()).thenReturn(metaBuf("resp-2"));

    try (ScriptedResultSet resultSet = new ScriptedResultSet(first, second)) {
      resultSet.execute();
      assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), readAll(resultSet));
      // the ticket was replayed exactly once, continuing after the last acknowledged batch
      assertEquals(Arrays.asList("resp-1"), resultSet.resumeRequests);
      assertEquals(1, resultSet.getResumeAttempts());
      assertEquals("resp-2", resultSet.getLastResponseId());
    }
  }

  @Test
  public void shouldFailWithoutResponseIdCaptured() throws Exception {
    final FlightRuntimeException broken =
        CallStatus.UNAVAILABLE.withDescription("stream broken").toRuntimeException();
    final FlightStream stream = streamOf(root());
    when(stream.next()).thenThrow(broken);

    try (ScriptedResultSet resultSet = new ScriptedResultSet(stream)) {
      resultSet.execute();
      final SQLException exception = assertThrows(SQLException.class, resultSet::next);
      assertTrue(exception.getCause() instanceof FlightRuntimeException);
      // no batch acknowledged, so there is nothing to resume from
      assertTrue(resultSet.resumeRequests.isEmpty());
      assertEquals(0, resultSet.getResumeAttempts());
    }
  }

  @Test
  public void shouldFailAfterExhaustingResumeBudget() throws Exception {
    final FlightRuntimeException broken =
        CallStatus.UNAVAILABLE.withDescription("stream broken").toRuntimeException();

    final VectorSchemaRoot root1 = root();
    final FlightStream first = streamOf(root1);
    when(first.next())
        .thenAnswer(
            inv -> {
              fill(root1, 1, 2);
              return true;
            })
        .thenThrow(broken);
    when(first.getLatestMetadata()).thenReturn(metaBuf("resp-1"));

    // three resumed streams, each dying before delivering anything
    final FlightStream dying1 = streamOf(root());
    final FlightStream dying2 = streamOf(root());
    final FlightStream dying3 = streamOf(root());
    when(dying1.next()).thenThrow(broken);
    when(dying2.next()).thenThrow(broken);
    when(dying3.next()).thenThrow(broken);

    try (ScriptedResultSet resultSet =
        new ScriptedResultSet(first, dying1, dying2, dying3)) {
      resultSet.execute();
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getInt(1));
      assertTrue(resultSet.next());
      assertEquals(2, resultSet.getInt(1));
      assertThrows(SQLException.class, resultSet::next);
      // all three resumes replay from the single acknowledged batch
      assertEquals(Arrays.asList("resp-1", "resp-1", "resp-1"), resultSet.resumeRequests);
      assertEquals(3, resultSet.getResumeAttempts());
    }
  }

  @Test
  public void shouldRejectScrollStatementOnInlineConnection() throws Exception {
    try (Connection connection = inlineConnection();
        Statement statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      final SQLException exception =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM TEST"));
      assertTrue(
          exception.getMessage().contains("mutually exclusive"),
          "unexpected exception: " + exception);
    }
  }

  @Test
  public void shouldFallBackToStreamResultSetWhenServerReturnsNoInlineTicket() throws Exception {
    // the mock producer ignores x-fg-endpoint-mode and returns regular endpoints, so the driver
    // must fall back to the regular streaming result set instead of the inline one
    try (Connection connection = inlineConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST")) {
      assertTrue(resultSet instanceof ArrowFlightJdbcFlightStreamResultSet);
      assertTrue(resultSet.next());
    }
  }

  /** Connects with {@code x-fg-endpoint-mode=inline} declared in the JDBC URL. */
  private Connection inlineConnection() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("user", FlightServerTestExtension.DEFAULT_USER);
    properties.setProperty("password", FlightServerTestExtension.DEFAULT_PASSWORD);
    properties.setProperty("useEncryption", "false");
    return DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s;x-fg-endpoint-mode=inline",
            FLIGHT_SERVER_TEST_EXTENSION.getPort()),
        properties);
  }

  @Test
  public void shouldDetectInlineTickets() {
    assertTrue(isInline("INLINE|bucket|prefix|q|user|1|-|hmac"));
    assertFalse(isInline("STREAM|bucket|prefix|q|user|1|-|hmac"));
    assertFalse(isInline("INLINE"));
    assertFalse(isInline(""));

    final FlightInfo noEndpoints = mock(FlightInfo.class);
    when(noEndpoints.getEndpoints()).thenReturn(List.of());
    assertFalse(ArrowFlightJdbcInlineResultSet.isInlineTicket(noEndpoints));

    final FlightInfo nullTicket = mock(FlightInfo.class);
    final FlightEndpoint endpoint = mock(FlightEndpoint.class);
    when(endpoint.getTicket()).thenReturn(null);
    when(nullTicket.getEndpoints()).thenReturn(List.of(endpoint));
    assertFalse(ArrowFlightJdbcInlineResultSet.isInlineTicket(nullTicket));

    assertFalse(ArrowFlightJdbcInlineResultSet.isInlineTicket(null));
  }

  private boolean isInline(final String ticket) {
    final FlightInfo info = mock(FlightInfo.class);
    when(info.getEndpoints())
        .thenReturn(
            List.of(new FlightEndpoint(new Ticket(ticket.getBytes(StandardCharsets.UTF_8)))));
    return ArrowFlightJdbcInlineResultSet.isInlineTicket(info);
  }

  private List<Integer> readAll(final ArrowFlightJdbcInlineResultSet resultSet)
      throws SQLException {
    final List<Integer> values = new ArrayList<>();
    while (resultSet.next()) {
      values.add(resultSet.getInt(1));
    }
    return values;
  }

  private Meta.Signature testSignature() {
    return ArrowFlightMetaImpl.newSignature(null, null, null);
  }

  private FlightInfo inlineInfo() {
    final FlightInfo info = mock(FlightInfo.class);
    when(info.getEndpoints())
        .thenReturn(
            List.of(
                new FlightEndpoint(
                    new Ticket(INLINE_TICKET.getBytes(StandardCharsets.UTF_8)))));
    when(info.getSchemaOptional()).thenReturn(Optional.of(schema));
    return info;
  }

  private VectorSchemaRoot root() {
    final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    rootsToClose.add(root);
    return root;
  }

  private FlightStream streamOf(final VectorSchemaRoot root) {
    final FlightStream stream = mock(FlightStream.class);
    when(stream.getRoot()).thenReturn(root);
    return stream;
  }

  /** Loads the given rows into the root, simulating a batch arriving on the DoGet stream. */
  private void fill(final VectorSchemaRoot root, final int... values) {
    root.clear();
    final IntVector vector = (IntVector) root.getVector("id");
    for (int i = 0; i < values.length; i++) {
      vector.setSafe(i, values[i]);
    }
    vector.setValueCount(values.length);
    root.setRowCount(values.length);
  }

  /** An ArrowBuf carrying {@code value} as app_metadata, like the gateway's responseId stamps. */
  private ArrowBuf metaBuf(final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final ArrowBuf buf = allocator.buffer(bytes.length);
    buf.setBytes(0, bytes);
    buf.writerIndex(bytes.length);
    buffersToClose.add(buf);
    return buf;
  }
}
