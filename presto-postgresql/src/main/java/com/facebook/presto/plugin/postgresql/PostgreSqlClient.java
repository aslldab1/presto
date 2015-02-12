/*
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
package com.facebook.presto.plugin.postgresql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import javax.inject.Inject;

import org.postgresql.Driver;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.optimizations.estimater.SemijoinMetadata;
import com.google.common.base.Throwables;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new Driver());
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<String> fragments)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
    
    @Override
    public SemijoinMetadata getSemijoinMetadata(SchemaTableName table) {
    	try (Connection connection = driver.connect(connectionUrl,
				connectionProperties)) {
    		SemijoinMetadata metadata = new SemijoinMetadata();
			String schemaName = table.getSchemaName();
			String tableName = table.getTableName();
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columns = meta.getColumns(null, schemaName, tableName, null);
			while(columns.next())
			{
				metadata.putColumnMap(columns.getString("COLUMN_NAME"), columns.getInt("COLUMN_SIZE"));
			}
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement
						.executeQuery("SELECT reltuples FROM pg_class r JOIN pg_namespace n ON (relnamespace = n.oid) WHERE relkind = 'r' AND n.nspname = '" + schemaName + "' and relname = '" + tableName + "'");
				if (resultSet.next())
					metadata.setRowCount(resultSet.getLong(1));
			}
			return metadata;
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
    }
}
