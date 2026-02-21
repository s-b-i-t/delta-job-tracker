package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

public class V12__workday_invalid_url_cleanup extends BaseJavaMigration {
  private static final String CONSTRAINT_NAME = "job_postings_canonical_url_not_invalid";
  private static final String INVALID_PREFIX = "https://community.workday.com/invalid-url%";

  @Override
  public void migrate(Context context) throws Exception {
    try (Statement statement = context.getConnection().createStatement()) {
      statement.executeUpdate(
          "DELETE FROM job_postings "
              + "WHERE canonical_url LIKE '"
              + INVALID_PREFIX
              + "' "
              + "OR source_url LIKE '"
              + INVALID_PREFIX
              + "'");
    }

    if (!constraintExists(context.getConnection())) {
      try (Statement statement = context.getConnection().createStatement()) {
        statement.executeUpdate(
            "ALTER TABLE job_postings "
                + "ADD CONSTRAINT "
                + CONSTRAINT_NAME
                + " "
                + "CHECK (canonical_url IS NULL OR canonical_url NOT LIKE '"
                + INVALID_PREFIX
                + "')");
      }
    }
  }

  private boolean constraintExists(Connection connection) throws SQLException {
    String sql =
        "SELECT 1 FROM information_schema.table_constraints "
            + "WHERE LOWER(table_name) = 'job_postings' "
            + "AND LOWER(constraint_name) = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, CONSTRAINT_NAME.toLowerCase());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }
}
