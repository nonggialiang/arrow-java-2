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

import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forward-only {@link java.sql.ResultSet} for the FG gateway's inline streaming mode.
 *
 * <p>The gateway (queried with {@code x-fg-endpoint-mode=inline}) returns a single {@code INLINE|}
 * prefixed ticket from GetFlightInfo and then proxies rows batch-by-batch over DoGet without
 * materializing them. This ResultSet consumes that DoGet stream synchronously and, after every
 * batch, stashes the responseId the gateway stamps into the batch's {@code app_metadata}. If the
 * stream dies mid-flight, the ticket is re-presented on DoGet with an {@code
 * x-fg-resume-response-id} header so the gateway continues after the last acknowledged batch; the
 * number of such resumes is bounded ({@value #MAX_RESUME_ATTEMPTS}).
 *
 * <p>Inline is mutually exclusive with {@code TYPE_SCROLL_INSENSITIVE} (scroll mode); the factory
 * rejects that combination before execution.
 */
public class ArrowFlightJdbcInlineResultSet extends ArrowFlightJdbcVectorSchemaRootResultSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightJdbcInlineResultSet.class);

  /**
   * Wire prefix of gateway relay tickets of kind INLINE ({@code INLINE|bucket|...|hmac}, see the
   * gateway's RelayTicketCodec).
   */
  static final String INLINE_TICKET_PREFIX = "INLINE|";

  /** Upper bound on resume attempts after mid-stream failures. */
  private static final int MAX_RESUME_ATTEMPTS = 3;

  private final ArrowFlightInfoStatement statement;
  private final Ticket ticket;
  private final Schema schema;

  private FlightStream stream;
  private VectorSchemaRoot currentRoot;
  private volatile String lastResponseId;
  private int resumeAttempts;

  ArrowFlightJdbcInlineResultSet(
      final AvaticaStatement statement,
      final QueryState state,
      final Signature signature,
      final ResultSetMetaData resultSetMetaData,
      final TimeZone timeZone,
      final Frame firstFrame,
      final FlightInfo info)
      throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.statement = (ArrowFlightInfoStatement) statement;
    if (info.getEndpoints().isEmpty() || info.getEndpoints().get(0).getTicket() == null) {
      throw new SQLException("Inline execution returned no ticket endpoint");
    }
    this.ticket = info.getEndpoints().get(0).getTicket();
    this.schema = info.getSchemaOptional().orElse(null);
  }

  /**
   * Whether the given {@link FlightInfo} carries an INLINE ticket endpoint, i.e. the gateway
   * honored {@code x-fg-endpoint-mode=inline} and returned a proxy-stream ticket.
   */
  static boolean isInlineTicket(final FlightInfo info) {
    if (info == null) {
      return false;
    }
    final List<FlightEndpoint> endpoints = info.getEndpoints();
    if (endpoints.isEmpty()) {
      return false;
    }
    final Ticket endpointTicket = endpoints.get(0).getTicket();
    if (endpointTicket == null) {
      return false;
    }
    final byte[] bytes = endpointTicket.getBytes();
    if (bytes.length < INLINE_TICKET_PREFIX.length()) {
      return false;
    }
    return INLINE_TICKET_PREFIX.equals(
        new String(bytes, 0, INLINE_TICKET_PREFIX.length(), StandardCharsets.UTF_8));
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    if (schema != null) {
      try {
        this.stream = openStream(null);
        this.currentRoot = stream.getRoot();
      } catch (final FlightRuntimeException e) {
        throw new SQLException("Failed to open inline stream", e);
      }
      populateData(currentRoot, schema);
    }
    return this;
  }

  /** The last responseId captured from batch app_metadata; null before the first batch. */
  public String getLastResponseId() {
    return lastResponseId;
  }

  @Override
  public boolean next() throws SQLException {
    if (currentRoot == null) {
      return false;
    }
    while (true) {
      final boolean hasNext = super.next();
      final int maxRows = statement != null ? statement.getMaxRows() : 0;
      if (maxRows != 0 && this.getRow() > maxRows) {
        if (statement.isCloseOnCompletion()) {
          statement.close();
        }
        return false;
      }

      if (hasNext) {
        return true;
      }

      if (stream == null) {
        break;
      }

      currentRoot.clear();
      if (loadNextBatch()) {
        continue;
      }
      break;
    }

    if (statement != null && statement.isCloseOnCompletion()) {
      statement.close();
    }
    return false;
  }

  /**
   * Pulls the next batch from the inline DoGet stream, resuming across mid-stream failures when a
   * responseId has been captured.
   *
   * @return true if a new batch was loaded into the current root; false when the stream is
   *     exhausted.
   */
  private boolean loadNextBatch() throws SQLException {
    while (true) {
      final boolean advanced;
      try {
        advanced = stream.next();
      } catch (final FlightRuntimeException e) {
        if (tryResume(e)) {
          continue;
        }
        throw new SQLException("Inline stream failed and could not be resumed", e);
      }
      if (advanced) {
        // Stash the responseId immediately: the buffer may be cleared by a subsequent next().
        updateLastResponseId(stream.getLatestMetadata());
        currentRoot = stream.getRoot();
        populateData(currentRoot, schema);
        return true;
      }
      // Stream completed cleanly; nothing more to consume or resume onto.
      closeStreamQuietly();
      stream = null;
      return false;
    }
  }

  /**
   * Attempts to re-open the inline stream after a mid-stream failure, asking the gateway to
   * continue after {@link #lastResponseId}.
   *
   * @return true if a fresh stream was opened; false when no responseId has been captured or the
   *     retry budget is exhausted.
   */
  private boolean tryResume(final FlightRuntimeException cause) throws SQLException {
    final String responseId = lastResponseId;
    if (responseId == null) {
      return false;
    }
    if (resumeAttempts >= MAX_RESUME_ATTEMPTS) {
      LOGGER.warn(
          "Inline stream resume budget exhausted ({} attempts), failing the result set",
          MAX_RESUME_ATTEMPTS,
          cause);
      return false;
    }
    resumeAttempts++;
    LOGGER.warn(
        "Inline stream interrupted, resuming from responseId '{}' (attempt {}/{})",
        responseId,
        resumeAttempts,
        MAX_RESUME_ATTEMPTS,
        cause);
    closeStreamQuietly();
    stream = openStream(responseId);
    return true;
  }

  /**
   * Opens the DoGet stream for the INLINE ticket. When {@code resumeResponseId} is non-null the
   * gateway is asked to continue after that cursor instead of replaying from the start. Subclasses
   * may override to customize the per-call options.
   */
  protected FlightStream openStream(final String resumeResponseId) throws SQLException {
    final List<CallOption> extraOptions;
    if (resumeResponseId == null) {
      extraOptions = Collections.emptyList();
    } else {
      final CallHeaders headers = new FlightCallHeaders();
      headers.insert(
          ArrowFlightConnectionConfigImpl.HEADER_FG_RESUME_RESPONSE_ID, resumeResponseId);
      extraOptions = List.of(new HeaderCallOption(headers));
    }
    try {
      return statement.getConnection().getClientHandler().getStream(ticket, extraOptions);
    } catch (final FlightRuntimeException e) {
      throw new SQLException("Failed to open inline stream", e);
    }
  }

  /** Captures the responseId the gateway stamped into the batch's app_metadata, if any. */
  private void updateLastResponseId(final ArrowBuf metadata) {
    if (metadata == null) {
      return;
    }
    final int length = (int) metadata.readableBytes();
    if (length <= 0) {
      return;
    }
    final byte[] bytes = new byte[length];
    metadata.getBytes(metadata.readerIndex(), bytes);
    final String value = new String(bytes, StandardCharsets.UTF_8).trim();
    if (!value.isEmpty()) {
      this.lastResponseId = value;
    }
  }

  private void closeStreamQuietly() {
    final FlightStream current = this.stream;
    if (current != null) {
      try {
        current.close();
      } catch (final Exception e) {
        LOGGER.debug("Suppressed inline stream close failure", e);
      }
    }
  }

  @Override
  protected void cancel() {
    super.cancel();
    final FlightStream current = this.stream;
    if (current != null) {
      current.cancel("Cancel", null);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (isClosed()) {
        return;
      }
    } catch (final SQLException e) {
      // fall through and release the stream regardless
    }
    closeStreamQuietly();
    currentRoot = null;
    super.close();
  }

  /** Number of resume attempts performed so far; exposed for observability and tests. */
  int getResumeAttempts() {
    return resumeAttempts;
  }
}
