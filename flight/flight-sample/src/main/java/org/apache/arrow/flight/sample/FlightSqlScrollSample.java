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
package org.apache.arrow.flight.sample;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcFlightScrollResultSet;

/**
 * Sample that runs a query through the Arrow Flight SQL JDBC driver with a {@link
 * ResultSet#TYPE_SCROLL_INSENSITIVE} statement, then reads the result in random page order using
 * {@code absolute()} to seek and {@code next()} to fetch rows, checksums every row and reports
 * per-page timing statistics.
 *
 * <p>Usage: {@code FlightSqlScrollSample <jdbcUrl> <user> <password> <pageSize> <sql>}
 */
public final class FlightSqlScrollSample {

  private static final String USAGE =
      "Usage: FlightSqlScrollSample <jdbcUrl> <user> <password> <pageSize> <sql>\n"
          + "  jdbcUrl   e.g. jdbc:arrow-flight-sql://localhost:47470\n"
          + "  pageSize  page size for server-side paging, also set as the statement fetch size\n"
          + "  sql       query to execute";

  private FlightSqlScrollSample() {}

  /**
   * Entry point.
   *
   * @param args jdbcUrl, user, password, pageSize, sql
   */
  public static void main(String[] args) {
    if (args.length != 5) {
      System.err.println(USAGE);
      System.exit(1);
    }
    final String url = args[0];
    final String user = args[1];
    final String password = args[2];
    final int pageSize = Integer.parseInt(args[3]);
    final String sql = args[4];
    if (pageSize <= 0) {
      System.err.println("pageSize must be positive, got: " + pageSize);
      System.exit(1);
    }
    try {
      run(url, user, password, pageSize, sql);
    } catch (SQLException e) {
      System.err.println("Sample failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void run(String url, String user, String password, int pageSize, String sql)
      throws SQLException {
    try (Connection connection = DriverManager.getConnection(url, user, password);
        Statement statement =
            connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      statement.setFetchSize(pageSize);
      System.out.println("executing SQL: " + sql);
      final long executeStart = System.nanoTime();
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        System.out.printf(
            "SQL executed in %.3f ms%n", (System.nanoTime() - executeStart) / 1_000_000.0);
        if (!(resultSet instanceof ArrowFlightJdbcFlightScrollResultSet)) {
          throw new SQLException(
              "Expected ArrowFlightJdbcFlightScrollResultSet but got "
                  + resultSet.getClass().getName());
        }
        readRandomPages((ArrowFlightJdbcFlightScrollResultSet) resultSet, pageSize);
      }
    }
  }

  private static void readRandomPages(ArrowFlightJdbcFlightScrollResultSet resultSet, int pageSize)
      throws SQLException {
    final long totalRows = resultSet.getTotalRows();
    final int columnCount = resultSet.getMetaData().getColumnCount();
    final int pageCount = (int) ((totalRows + pageSize - 1) / pageSize);
    System.out.printf(
        "total rows: %d, columns: %d, page size: %d, pages: %d%n",
        totalRows, columnCount, pageSize, pageCount);

    final List<Integer> pages = new ArrayList<>(pageCount);
    for (int i = 0; i < pageCount; i++) {
      pages.add(i);
    }
    Collections.shuffle(pages);

    final CRC32 checksum = new CRC32();
    final List<Double> pageMillis = new ArrayList<>(pageCount);
    long rowsRead = 0;
    for (int page : pages) {
      final long firstRow = (long) page * pageSize + 1L;
      final long rowsInPage = Math.min((long) pageSize, totalRows - (long) page * pageSize);
      final long start = System.nanoTime();
      // absolute() 定位到本页首行之前（第 0 页即 beforeFirst），随后用 next() 逐行读取
      resultSet.absolute((int) firstRow - 1);
      int fetched = 0;
      while (fetched < rowsInPage && resultSet.next()) {
        updateChecksum(resultSet, columnCount, checksum);
        fetched++;
      }
      final double millis = (System.nanoTime() - start) / 1_000_000.0;
      pageMillis.add(millis);
      rowsRead += fetched;
      System.out.printf(
          "page %d [rows %d..%d]: fetched %d rows in %.3f ms%n",
          page, firstRow, firstRow + rowsInPage - 1, fetched, millis);
    }

    System.out.printf("rows read: %d / %d%n", rowsRead, totalRows);
    System.out.printf("checksum: %s%n", Long.toHexString(checksum.getValue()));
    if (pageMillis.isEmpty()) {
      System.out.println("page time avg/min/max: n/a (no pages)");
    } else {
      final double avg = pageMillis.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      System.out.printf(
          "page time avg/min/max: %.3f / %.3f / %.3f ms%n",
          avg, Collections.min(pageMillis), Collections.max(pageMillis));
    }
  }

  private static void updateChecksum(ResultSet resultSet, int columnCount, CRC32 checksum)
      throws SQLException {
    for (int i = 1; i <= columnCount; i++) {
      final Object value = resultSet.getObject(i);
      checksum.update(
          (value == null ? "NULL" : String.valueOf(value)).getBytes(StandardCharsets.UTF_8));
      checksum.update((byte) '|');
    }
    checksum.update((byte) '\n');
  }
}
