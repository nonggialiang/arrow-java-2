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
 * Sample that runs a query through the Arrow Flight SQL JDBC driver.
 *
 * <p>In {@code scroll} mode the statement is created {@link ResultSet#TYPE_SCROLL_INSENSITIVE}, the
 * result is read in random page order using {@code absolute()} to seek and {@code next()} to fetch
 * rows, checksums every row and reports per-page timing statistics.
 *
 * <p>In {@code forward} mode the statement is created {@link ResultSet#TYPE_FORWARD_ONLY} and all
 * rows are read sequentially from start to end, checksummed and reported.
 *
 * <p>Usage: {@code FlightSqlScrollSample <jdbcUrl> <user> <password> <pageSize> <mode> <sql>}
 */
public final class FlightSqlScrollSample {

  private static final String USAGE =
      "Usage: FlightSqlScrollSample <jdbcUrl> <user> <password> <pageSize> <mode> <sql>\n"
          + "  jdbcUrl   e.g. jdbc:arrow-flight-sql://localhost:47470\n"
          + "  pageSize  page size for server-side paging, also set as the statement fetch size\n"
          + "  mode      scroll (TYPE_SCROLL_INSENSITIVE, random paged access) or\n"
          + "            forward (TYPE_FORWARD_ONLY, sequential read)\n"
          + "  sql       query to execute";

  private FlightSqlScrollSample() {}

  /**
   * Entry point.
   *
   * @param args jdbcUrl, user, password, pageSize, mode, sql
   */
  public static void main(String[] args) {
    if (args.length != 6) {
      System.err.println(USAGE);
      System.exit(1);
    }
    final String url = args[0];
    final String user = args[1];
    final String password = args[2];
    final int pageSize = Integer.parseInt(args[3]);
    final String mode = args[4];
    final String sql = args[5];
    if (pageSize <= 0) {
      System.err.println("pageSize must be positive, got: " + pageSize);
      System.exit(1);
    }
    final boolean scroll;
    if ("scroll".equalsIgnoreCase(mode)) {
      scroll = true;
    } else if ("forward".equalsIgnoreCase(mode)) {
      scroll = false;
    } else {
      System.err.println("mode must be 'scroll' or 'forward', got: " + mode);
      System.exit(1);
      return;
    }
    try {
      run(url, user, password, pageSize, scroll, sql);
    } catch (SQLException e) {
      System.err.println("Sample failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void run(
      String url, String user, String password, int pageSize, boolean scroll, String sql)
      throws SQLException {
    final int resultSetType =
        scroll ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
    try (Connection connection = DriverManager.getConnection(url, user, password);
        Statement statement =
            connection.createStatement(resultSetType, ResultSet.CONCUR_READ_ONLY)) {
      statement.setFetchSize(pageSize);
      System.out.println(
          "executing SQL: " + sql + " (mode: " + (scroll ? "scroll" : "forward") + ")");
      final long executeStart = System.nanoTime();
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        System.out.printf(
            "SQL executed in %.3f ms%n", (System.nanoTime() - executeStart) / 1_000_000.0);
        if (scroll) {
          if (!(resultSet instanceof ArrowFlightJdbcFlightScrollResultSet)) {
            throw new SQLException(
                "Expected ArrowFlightJdbcFlightScrollResultSet but got "
                    + resultSet.getClass().getName());
          }
          readRandomPages((ArrowFlightJdbcFlightScrollResultSet) resultSet, pageSize);
        } else {
          readSequentially(resultSet);
        }
      }
    }
  }

  private static void readSequentially(ResultSet resultSet) throws SQLException {
    final int columnCount = resultSet.getMetaData().getColumnCount();
    final CRC32 checksum = new CRC32();
    final long start = System.nanoTime();
    long rowsRead = 0;
    while (resultSet.next()) {
      updateChecksum(resultSet, columnCount, checksum);
      rowsRead++;
    }
    final double millis = (System.nanoTime() - start) / 1_000_000.0;
    System.out.printf("rows read: %d%n", rowsRead);
    System.out.printf("checksum: %s%n", Long.toHexString(checksum.getValue()));
    System.out.printf("read time: %.3f ms%n", millis);
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
