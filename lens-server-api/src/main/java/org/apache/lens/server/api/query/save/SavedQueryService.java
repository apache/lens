/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query.save;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.save.ListResponse;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.error.LensException;

public interface SavedQueryService {

  /**
   * The Constant NAME.
   */
  String NAME = "savedquery";

  /**
   * Saves a query
   * @param query  Saved query object.
   * @return id of the created saved query.
   * @throws LensException
   */
  long save(LensSessionHandle handle, SavedQuery query) throws LensException;

  /**
   * Updates the specified saved query with the new object.
   * @param handle session handle of the query
   * @param id     id of the saved query.
   * @param query  Saved query object.
   * @throws LensException
   */
  void update(LensSessionHandle handle, long id, SavedQuery query) throws LensException;

  /**
   * Deletes the saved query specified.
   * @param handle session handle of the query
   * @param id     id of the saved query.
   * @throws LensException
   */
  void delete(LensSessionHandle handle, long id) throws LensException;

  /**
   * Returns the saved query pointed out by the id.
   * @param handle session handle of the query
   * @param id     id of the saved query.
   * @return saved query object.
   * @throws LensException
   */
  SavedQuery get(LensSessionHandle handle, long id)  throws LensException;

  /**
   * List the saved query from {start} to {count} matching filter denoted by criteria.
   * @param handle    session handle of the query
   * @param criteria  Multivalued map representing the criteria.
   * @param start     Displacement from the first matching record.
   * @param count     Number of records to fetch.
   * @return list of queries.
   * @throws LensException
   */
  ListResponse list(LensSessionHandle handle, MultivaluedMap<String, String> criteria, long start,
                    long count) throws LensException;

  /**
   * Grant permissions for users to do actions on the saved query.
   * @param handle          session handle of the query
   * @param id              id of the query.
   * @param sharingUser     User invoking this action.
   * @param targetUserPath  Target users who have to get affected.
   * @param privileges      Privileges to be granted.
   * @throws LensException
   */
  void grant(LensSessionHandle handle, long id, String sharingUser, String targetUserPath,
             String[] privileges) throws LensException;

  /**
   * Revoke permissions from users to do actions on the saved query.
   * @param handle          session handle of the query
   * @param id              id of the query.
   * @param sharingUser     User invoking this action.
   * @param targetUserPath  Target users who have to get affected.
   * @param privileges      Privileges to be granted.
   * @throws LensException
   */
  void revoke(LensSessionHandle handle, long id, String sharingUser, String targetUserPath,
              String[] privileges) throws LensException;
}
