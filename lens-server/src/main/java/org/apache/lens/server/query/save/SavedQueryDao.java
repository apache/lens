/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.save;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.query.save.ListResponse;
import org.apache.lens.api.query.save.Parameter;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.save.exception.SavedQueryNotFound;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.StringEscapeUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;

public class SavedQueryDao {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String VALUE_ALIAS = "value_alias";
  private static final String SAVED_QUERY_TABLE_NAME = "saved_query";
  private static final String ID_COL_NAME = "id";
  private static final String NAME_COL_NAME = "name";
  private static final String DESCRIPTION_COL_NAME = "description";
  private static final String QUERY_COL_NAME = "query";
  private static final String PARAMS_COL_NAME = "params_json";
  private static final String CREATED_AT_COL_NAME = "created_at";
  private static final String UPDATED_AT_COL_NAME = "updated_at";

  private final QueryRunner runner;
  private final Dialect dialect;

  SavedQueryDao(String dialectClass, QueryRunner runner)
    throws LensException {
    try {
      this.runner = runner;
      this.dialect = (Dialect) Class.forName(dialectClass).newInstance();
      createSavedQueryTableIfNotExists();
    } catch (Exception e) {
      throw new LensException("Error initializing saved query dao", e);
    }
  }

  /**
   * Creates the saved query table
   *
   * @throws LensException cannot create saved query table
   */
  public void createSavedQueryTableIfNotExists() throws LensException {
    try {
      runner.update(dialect.getCreateTableSyntax());
    } catch (SQLException e) {
      throw new LensException("Cannot create saved query table!", e);
    }
  }

  /**
   * Saves the query passed
   *
   * @param savedQuery
   * @return insert id
   * @throws LensException
   */
  public long saveQuery(SavedQuery savedQuery) throws LensException {
    try {
      final ECMAEscapedSavedQuery ecmaEscaped = ECMAEscapedSavedQuery.getFor(savedQuery);
      runner.update(
        "insert into " + SAVED_QUERY_TABLE_NAME + " values (" + dialect.getAutoIncrementId(runner) + ", "
          + "'" + ecmaEscaped.getName() + "'"
          + ", "
          + "'" + ecmaEscaped.getDescription() + "'"
          + ","
          + "'" + ecmaEscaped.getQuery() + "'"
          + ","
          + "'" + ecmaEscaped.getParameters() + "'"
          + ","
          + "now()"
          + ","
          + "now()"
          + ")"
      );
      return dialect.getLastInsertedID(runner);
    } catch (SQLException e) {
      throw new LensException("Save query failed !", e);
    }
  }

  /**
   * Updates the saved query id with new payload
   *
   * @param id
   * @param savedQuery
   * @throws LensException
   */
  public void updateQuery(long id, SavedQuery savedQuery) throws LensException {
    try {
      final ECMAEscapedSavedQuery ecmaEscaped = ECMAEscapedSavedQuery.getFor(savedQuery);
      final int rowsUpdated = runner.update(
        "update " + SAVED_QUERY_TABLE_NAME +"  set "
          + NAME_COL_NAME + " = '" + ecmaEscaped.getName() + "',"
          + DESCRIPTION_COL_NAME + " = '" + ecmaEscaped.getDescription() + "',"
          + QUERY_COL_NAME + " = '" + ecmaEscaped.getQuery() + "',"
          + PARAMS_COL_NAME + " = '" + ecmaEscaped.getParameters() + "',"
          + UPDATED_AT_COL_NAME + " = now() "
          + "where " + ID_COL_NAME + " = " + id
      );
      if (rowsUpdated == 0) {
        throw new SavedQueryNotFound(id);
      }
    } catch (SQLException e) {
      throw new LensException("Update failed for " + id, e);
    }
  }

  /**
   * Gets saved query with the given id
   *
   * @param id
   * @return
   * @throws LensException
   */
  public SavedQuery getSavedQueryByID(long id) throws LensException {
    final List<SavedQuery> savedQueries;
    try {
      savedQueries = runner.query(
        "select * from " + SAVED_QUERY_TABLE_NAME + " where " + ID_COL_NAME + " = " + id,
        new SavedQueryResultSetHandler()
      );
    } catch (SQLException e) {
      throw new LensException("Get failed for " + id, e);
    }
    int size = savedQueries.size();
    switch (size) {
    case 0:
      throw new SavedQueryNotFound(id);
    case 1:
      return savedQueries.get(0);
    default:
      throw new RuntimeException("More than one obtained for id, Please check the integrity of the data!");
    }
  }

  /**
   * Returns a list of saved queries
   *
   * @param criteria  a multivalued map that has the filter criteria
   * @param start     Displacement from the start of the search result
   * @param count     Count of number of records required
   * @return list of saved queries
   * @throws LensException
   */
  public ListResponse getList(
    MultivaluedMap<String, String> criteria, long start, long count) throws LensException {
    final StringBuilder selectQueryBuilder = new StringBuilder("select * from " + SAVED_QUERY_TABLE_NAME);
    final Set<String> availableFilterKeys = FILTER_KEYS.keySet();
    final Sets.SetView<String> intersection = Sets.intersection(availableFilterKeys, criteria.keySet());
    if (intersection.size() > 0) {
      final StringBuilder whereClauseBuilder = new StringBuilder(" where ");
      final List<String> predicates = Lists.newArrayList();
      for (String colName : intersection) {
        predicates.add(
          FILTER_KEYS.get(colName)
            .resolveFilterExpression(
              colName,
              criteria.getFirst(colName)
            )
        );
      }
      Joiner.on(" and ").skipNulls().appendTo(whereClauseBuilder, predicates);
      selectQueryBuilder.append(whereClauseBuilder.toString());
    }
    final String listCountQuery = "select count(*) as " + VALUE_ALIAS
      + " from (" + selectQueryBuilder.toString() + ") tmp_table";
    selectQueryBuilder
      .append(" limit ")
      .append(start)
      .append(", ")
      .append(count);
    final String listQuery = selectQueryBuilder.toString();
    try {
      return new ListResponse(
        start,
        runner.query(listCountQuery, new SingleValuedResultHandler()),
        runner.query(listQuery, new SavedQueryResultSetHandler())
      );
    } catch (SQLException e) {
      throw new LensException("List query failed!", e);
    }
  }

  /**
   * Deletes the saved query with the given id
   *
   * @param id
   * @throws LensException
   */
  public void deleteSavedQueryByID(long id) throws LensException {
    try {
      int rowsDeleted = runner.update(
        "delete from " + SAVED_QUERY_TABLE_NAME +" where " + ID_COL_NAME + " = " + id
      );
      if (rowsDeleted == 0) {
        throw new SavedQueryNotFound(id);
      } else if (rowsDeleted > 1) {
        throw new LensException("Warning! More than one record was deleted", new Throwable());
      }
    } catch (SQLException e) {
      throw new LensException("Delete query failed", e);
    }
  }

  /**
   * The interface Dialect.
   */
  public interface Dialect {
    /**
     * The create table syntax for 'this' dialect
     * @return
     */
    String getCreateTableSyntax();

    /**
     * Method to get the auto increment id/keyword(null) for the ID column
     * @param runner
     * @return
     * @throws SQLException
     */
    Long getAutoIncrementId(QueryRunner runner)  throws SQLException;

    /**
     * Get the last increment id after doing an auto increment
     * @param runner
     * @return
     * @throws SQLException
     */
    Long getLastInsertedID(QueryRunner runner) throws SQLException;
  }

  /**
   * MySQL dialect for saved query.
   */
  public static class MySQLDialect implements Dialect {

    @Override
    public String getCreateTableSyntax() {
      return "CREATE TABLE IF NOT EXISTS " + SAVED_QUERY_TABLE_NAME + " ("
        + ID_COL_NAME + " int(11) NOT NULL AUTO_INCREMENT,"
        + NAME_COL_NAME + " varchar(255) NOT NULL,"
        + DESCRIPTION_COL_NAME + " varchar(255) DEFAULT NULL,"
        + QUERY_COL_NAME + " longtext,"
        + PARAMS_COL_NAME + " longtext,"
        + CREATED_AT_COL_NAME + " timestamp DEFAULT CURRENT_TIMESTAMP,"
        + UPDATED_AT_COL_NAME + " timestamp NOT NULL,"
        + "  PRIMARY KEY ("+ ID_COL_NAME +")"
        + ")";

    }

    @Override
    public Long getAutoIncrementId(QueryRunner runner) throws SQLException {
      return null;
    }

    @Override
    public Long getLastInsertedID(QueryRunner runner) throws SQLException {
      return runner.query(
        "select last_insert_id() as " + VALUE_ALIAS,
        new SingleValuedResultHandler()
      );
    }
  }

  /**
   * HSQL dialect for saved query (Used with testing).
   */
  public static class HSQLDialect implements Dialect {

    @Override
    public String getCreateTableSyntax() {
      return "CREATE TABLE if not exists " + SAVED_QUERY_TABLE_NAME + "  ("
        + ID_COL_NAME + " int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
        + NAME_COL_NAME + " varchar(255), "
        + DESCRIPTION_COL_NAME + " varchar(255), "
        + QUERY_COL_NAME + " varchar(255), "
        + PARAMS_COL_NAME + " varchar(255), "
        + CREATED_AT_COL_NAME + " timestamp, "
        + UPDATED_AT_COL_NAME + " timestamp)";

    }

    @Override
    public Long getAutoIncrementId(QueryRunner runner) throws SQLException {
      return runner.query("select max(" + ID_COL_NAME + ")  as " + VALUE_ALIAS +" from " + SAVED_QUERY_TABLE_NAME
        , new SingleValuedResultHandler()) + 1;
    }

    @Override
    public Long getLastInsertedID(QueryRunner runner) throws SQLException {
      Long id = runner.query("select max(" + ID_COL_NAME + ")  as " + VALUE_ALIAS + " from " + SAVED_QUERY_TABLE_NAME
        , new SingleValuedResultHandler());
      if (id == 0) {
        id++;
      }
      return id;
    }
  }

  /**
   * Result set handler class to get a saved query from result set
   */
  public static class SavedQueryResultSetHandler implements ResultSetHandler<List<SavedQuery>> {

    @Override
    public List<SavedQuery> handle(ResultSet resultSet) throws SQLException {
      List<SavedQuery> queries = Lists.newArrayList();
      while (resultSet.next()) {
        long id = resultSet.getLong(ID_COL_NAME);
        final String name = StringEscapeUtils.unescapeEcmaScript(resultSet.getString(NAME_COL_NAME));
        final String description = StringEscapeUtils.unescapeEcmaScript(resultSet.getString(DESCRIPTION_COL_NAME));
        final String query = StringEscapeUtils.unescapeEcmaScript(resultSet.getString(QUERY_COL_NAME));
        final List<Parameter> parameterList;
        try {
          parameterList = deserializeFrom(
            StringEscapeUtils.unescapeEcmaScript(resultSet.getString(PARAMS_COL_NAME))
          );
        } catch (LensException e) {
          throw new SQLException("Cannot deserialize parameters ", e);
        }
        queries.add(new SavedQuery(
          id,
          name,
          description,
          query,
          parameterList
        ));
      }
      return queries;
    }
  }

  /**
   * Result set handler class to get a the last inserted ID from the resultset
   */
  public static class SingleValuedResultHandler implements ResultSetHandler<Long> {

    @Override
    public Long handle(ResultSet resultSet) throws SQLException {
      while (resultSet.next()) {
        return resultSet.getLong(VALUE_ALIAS);
      }
      throw new SQLException("For cursor : " + resultSet.getCursorName());
    }
  }

  @AllArgsConstructor
  @Data
  /**
   * This class represents a ECMA escaped version of saved query,
   * that can be safely inserted into DB
   */
  private static class ECMAEscapedSavedQuery {
    private final long id;
    private final String name;
    private final String description;
    private final String query;
    private final String parameters;

    static ECMAEscapedSavedQuery getFor(SavedQuery savedQuery) throws LensException {
      return new ECMAEscapedSavedQuery(
        savedQuery.getId(),
        StringEscapeUtils.escapeEcmaScript(savedQuery.getName()),
        StringEscapeUtils.escapeEcmaScript(savedQuery.getDescription()),
        StringEscapeUtils.escapeEcmaScript(savedQuery.getQuery()),
        StringEscapeUtils.escapeEcmaScript(serializeParameters(savedQuery))
      );
    }
  }

  /**
   * The filter data type used in the list api
   */
  enum FilterDataType {
    STRING {
      String resolveFilterExpression(String col, String val) {
        return " " + col + " like '%" + val + "%'";
      }
    },
    NUMBER {
      String resolveFilterExpression(String col, String val) {
        return col + "=" + Long.parseLong(val);
      }
    },
    BOOLEAN {
      String resolveFilterExpression(String col, String val) {
        return col + "=" + Boolean.parseBoolean(val);
      }
    };

    abstract String resolveFilterExpression(String col, String val);
  }

  /**
   * Map of available filter keys and their data types
   * The list api can have filter criteria based on these keys.
   */
  private static final ImmutableMap<String, FilterDataType> FILTER_KEYS;

  static {
    final ImmutableMap.Builder<String, FilterDataType> filterValuesBuilder = ImmutableMap.builder();
    filterValuesBuilder.put(NAME_COL_NAME, FilterDataType.STRING);
    filterValuesBuilder.put(DESCRIPTION_COL_NAME, FilterDataType.STRING);
    filterValuesBuilder.put(QUERY_COL_NAME, FilterDataType.STRING);
    filterValuesBuilder.put(ID_COL_NAME, FilterDataType.NUMBER);
    FILTER_KEYS = filterValuesBuilder.build();
  }


  /**
   * Serializes the parameters of saved query using jackson
   *
   * @param savedQuery
   * @return
   * @throws LensException
   */
  private static String serializeParameters(SavedQuery savedQuery) throws LensException {
    final String paramsJson;
    try {
      paramsJson = MAPPER.writeValueAsString(savedQuery.getParameters());
    } catch (JsonProcessingException e) {
      throw new LensException("Serialization failed for " + savedQuery.getParameters(), e);
    }
    return paramsJson;
  }

  /**
   * Deserializes the parameters from string using jackson
   *
   * @param paramsJson
   * @return
   * @throws LensException
   */
  private static List<Parameter> deserializeFrom(String paramsJson) throws LensException {
    final Parameter[] parameterArray;
    try {
      parameterArray = MAPPER.readValue(paramsJson, Parameter[].class);
    } catch (IOException e) {
      throw new LensException("Failed to deserialize from " + paramsJson, e);
    }
    return Arrays.asList(parameterArray);
  }
}
