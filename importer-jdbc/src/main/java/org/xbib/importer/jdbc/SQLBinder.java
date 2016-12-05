package org.xbib.importer.jdbc;

import org.xbib.content.settings.Settings;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class SQLBinder {

    private static final Logger logger = Logger.getLogger(SQLBinder.class.getName());

    private Settings settings;

    private Calendar calendar;

    private JDBCState state;

    public SQLBinder(Settings settings, Calendar calendar, JDBCState state) {
        this.settings = settings;
        this.calendar = calendar;
        this.state = state;
    }

    public void bind(PreparedStatement statement, int i, Object object) throws SQLException {
        logger.log(Level.FINER, "bind: value = {}", object);
        Object value = state.interpolate(object);
        logger.log(Level.FINER, MessageFormat.format("bind: object = {0} interpolated to {1} class {2}", object,
                value, value != null ? value.getClass() : null));
        if (value == null) {
            statement.setNull(i, Types.VARCHAR);
        } else if (value instanceof String) {
            statement.setString(i, (String) value);
        } else if (value instanceof Integer) {
            statement.setInt(i, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(i, (Long) value);
        } else if (value instanceof BigDecimal) {
            statement.setBigDecimal(i, (BigDecimal) value);
        } else if (value instanceof java.sql.Date) {
            statement.setDate(i, (java.sql.Date) value);
        } else if (value instanceof Timestamp) {
            statement.setTimestamp(i, (Timestamp) value, calendar);
        } else if (value instanceof Float) {
            statement.setFloat(i, (Float) value);
        } else if (value instanceof Double) {
            statement.setDouble(i, (Double) value);
        } else {
            statement.setObject(i, value);
        }
    }

}
