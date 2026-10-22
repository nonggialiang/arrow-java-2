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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

/** Factory for the Arrow Flight JDBC Driver. */
public class ArrowFlightJdbcFactory implements AvaticaFactory {
  private final int major;
  private final int minor;

  // This need to be public so Avatica can call this constructor
  public ArrowFlightJdbcFactory() {
    this(4, 1);
  }

  private ArrowFlightJdbcFactory(final int major, final int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public AvaticaConnection newConnection(
      final UnregisteredDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties info)
      throws SQLException {
    return ArrowFlightConnection.createNewConnection(
        (ArrowFlightJdbcDriver) driver, factory, url, info, new RootAllocator(Long.MAX_VALUE));
  }

  @Override
  public AvaticaStatement newStatement(
      final AvaticaConnection connection,
      final Meta.StatementHandle handle,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability) {
    return new ArrowFlightStatement(
        (ArrowFlightConnection) connection,
        handle,
        resultType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public ArrowFlightPreparedStatement newPreparedStatement(
      final AvaticaConnection connection,
      final Meta.StatementHandle statementHandle,
      final Meta.Signature signature,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    final ArrowFlightConnection flightConnection = (ArrowFlightConnection) connection;
    ArrowFlightSqlClientHandler.PreparedStatement preparedStatement =
        flightConnection.getMeta().getPreparedStatement(statementHandle);

    return ArrowFlightPreparedStatement.newPreparedStatement(
        flightConnection,
        preparedStatement,
        statementHandle,
        signature,
        resultType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public AvaticaResultSet newResultSet(
      final AvaticaStatement statement,
      final QueryState state,
      final Meta.Signature signature,
      final TimeZone timeZone,
      final Meta.Frame frame)
      throws SQLException {
    final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);
    final FlightInfo info;
    final CallHeaders headers = new FlightCallHeaders();
    // use statementHandle(connectionId::id) as query id
    headers.insert(ArrowFlightConnectionConfigImpl.HEADER_FG_QUERY_ID, statement.handle.toString());
    // if connection with params x-fg-result-set-type=scroll
    boolean scrollable = statement.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE;
    // if statement created with params resultSetType=TYPE_SCROLL_INSENSITIVE
    if (scrollable) {
      headers.insert(
          ArrowFlightConnectionConfigImpl.HEADER_FG_RESULT_SET_TYPE,
          ArrowFlightConnectionConfigImpl.HEADER_FG_RESULT_SET_TYPE_SCROLL);
    }
    try {
      info =
          ((ArrowFlightInfoStatement) statement)
              .executeFlightInfoQuery(List.of(new HeaderCallOption(headers)));
    } catch (final SQLException e) {
      throw e;
    }
    if (scrollable) {
      return new ArrowFlightJdbcFlightScrollResultSet(
          statement, state, signature, metaData, timeZone, frame, info);
    }
    return new ArrowFlightJdbcFlightStreamResultSet(
        statement, state, signature, metaData, timeZone, frame, info);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(final AvaticaConnection connection) {
    return new ArrowDatabaseMetadata(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(
      final AvaticaStatement avaticaStatement, final Meta.Signature signature) {
    return new AvaticaResultSetMetaData(avaticaStatement, null, signature);
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }
}
