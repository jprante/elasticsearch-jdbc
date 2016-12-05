package org.xbib.importer.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Instant;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class SQLTypeParser {

    private static final Logger logger = Logger.getLogger(SQLTypeParser.class.getName());

    private final Locale locale;

    private Calendar calendar;

    private boolean shouldTreatBinaryAsString;

    private int scale;

    private int rounding;

    public SQLTypeParser(Locale locale, TimeZone timeZone,
                         boolean shouldTreatBinaryAsString, int scale, int rounding) {
        this.locale = locale != null ? locale : Locale.getDefault();
        Locale.setDefault(this.locale);
        TimeZone.setDefault(timeZone);
        this.calendar = Calendar.getInstance(timeZone, locale);
        logger.log(Level.INFO, "calendar timezone for JDBC timestamps = " + calendar.getTimeZone().getDisplayName());
        this.shouldTreatBinaryAsString = shouldTreatBinaryAsString;
        this.scale = scale;
        this.rounding = rounding;
    }

    public Calendar getCalendar() {
        return calendar;
    }

    /**
     * Parse of value of result set
     *
     * @param result the result set
     * @param i      the offset in the result set
     * @param type   the JDBC type
     * @return The parse value
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/output error occurs
     * @throws ParseException  when parse error occurs
     */
    public Object parseType(ResultSet result, Integer i, int type)
            throws SQLException, IOException, ParseException {
        logger.log(Level.FINER, "i=" + i + " type=" + type);
        switch (type) {
            /*
             * The JDBC types CHAR, VARCHAR, and LONGVARCHAR are closely
             * related. CHAR represents a small, fixed-length character string,
             * VARCHAR represents a small, variable-length character string, and
             * LONGVARCHAR represents a large, variable-length character string.
             */
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return result.getString(i);
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return result.getNString(i);
            /*
             * The JDBC types BINARY, VARBINARY, and LONGVARBINARY are closely
             * related. BINARY represents a small, fixed-length binary value,
             * VARBINARY represents a small, variable-length binary value, and
             * LONGVARBINARY represents a large, variable-length binary value
             */
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] b = result.getBytes(i);
                return shouldTreatBinaryAsString ? (b != null ? new String(b) : null) : b;
            /*
             * The JDBC type ARRAY represents the SQL3 type ARRAY.
             *
             * An ARRAY value is mapped to an instance of the Array interface in
             * the Java programming language. If a driver follows the standard
             * implementation, an Array object logically points to an ARRAY
             * value on the server rather than containing the elements of the
             * ARRAY object, which can greatly increase efficiency. The Array
             * interface contains methods for materializing the elements of the
             * ARRAY object on the client in the form of either an array or a
             * ResultSet object.
             */
            case Types.ARRAY:
                Array arr = result.getArray(i);
                return arr == null ? null : arr.getArray();

            /*
             * The JDBC type BIGINT represents a 64-bit signed integer value
             * between -9223372036854775808 and 9223372036854775807.
             *
             * The corresponding SQL type BIGINT is a nonstandard extension to
             * SQL. In practice the SQL BIGINT type is not yet currently
             * implemented by any of the major databases, and we recommend that
             * its use be avoided in code that is intended to be portable.
             *
             * The recommended Java mapping for the BIGINT type is as a Java
             * long.
             */
            case Types.BIGINT:
                Object o = result.getLong(i);
                return result.wasNull() ? null : o;
            /*
             * The JDBC type BIT represents a single bit value that can be zero
             * or one.
             *
             * SQL-92 defines an SQL BIT type. However, unlike the JDBC BIT
             * type, this SQL-92 BIT type can be used as a parameterized type to
             * define a fixed-length binary string. Fortunately, SQL-92 also
             * permits the use of the simple non-parameterized BIT type to
             * represent a single binary digit, and this usage corresponds to
             * the JDBC BIT type. Unfortunately, the SQL-92 BIT type is only
             * required in "full" SQL-92 and is currently supported by only a
             * subset of the major databases. Portable code may therefore prefer
             * to use the JDBC SMALLINT type, which is widely supported.
             */
            case Types.BIT:
                try {
                    o = result.getInt(i);
                    return result.wasNull() ? null : o;
                } catch (Exception e) {
                    String exceptionClassName = e.getClass().getName();
                    // postgresql can not handle boolean, it will throw PSQLException, something like "Bad value for type int : t"
                    if ("org.postgresql.util.PSQLException".equals(exceptionClassName)) {
                        return "t".equals(result.getString(i));
                    }
                    throw new IOException(e);
                }
            /*
             * The JDBC type BOOLEAN, which is new in the JDBC 3.0 API, maps to
             * a boolean in the Java programming language. It provides a
             * representation of true and false, and therefore is a better match
             * than the JDBC type BIT, which is either 1 or 0.
             */
            case Types.BOOLEAN:
                return result.getBoolean(i);
            /*
             * The JDBC type BLOB represents an SQL3 BLOB (Binary Large Object).
             *
             * A JDBC BLOB value is mapped to an instance of the Blob interface
             * in the Java programming language. If a driver follows the
             * standard implementation, a Blob object logically points to the
             * BLOB value on the server rather than containing its binary data,
             * greatly improving efficiency. The Blob interface provides methods
             * for materializing the BLOB data on the client when that is
             * desired.
             */
            case Types.BLOB:
                Blob blob = result.getBlob(i);
                if (blob != null) {
                    long n = blob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process blob larger than Integer.MAX_VALUE");
                    }
                    byte[] tab = blob.getBytes(1, (int) n);
                    blob.free();
                    return tab;
                }
                break;
            /*
             * The JDBC type CLOB represents the SQL3 type CLOB (Character Large
             * Object).
             *
             * A JDBC CLOB value is mapped to an instance of the Clob interface
             * in the Java programming language. If a driver follows the
             * standard implementation, a Clob object logically points to the
             * CLOB value on the server rather than containing its character
             * data, greatly improving efficiency. Two of the methods on the
             * Clob interface materialize the data of a CLOB object on the
             * client.
             */
            case Types.CLOB:
                Clob clob = result.getClob(i);
                if (clob != null) {
                    long n = clob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process clob larger than Integer.MAX_VALUE");
                    }
                    String str = clob.getSubString(1, (int) n);
                    clob.free();
                    return str;
                }
                break;
            case Types.NCLOB:
                NClob nclob = result.getNClob(i);
                if (nclob != null) {
                    long n = nclob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process nclob larger than Integer.MAX_VALUE");
                    }
                    String str = nclob.getSubString(1, (int) n);
                    nclob.free();
                    return str;
                }
                break;
            /*
             * The JDBC type DATALINK, new in the JDBC 3.0 API, is a column
             * value that references a file that is outside of a data source but
             * is managed by the data source. It maps to the Java type
             * java.net.URL and provides a way to manage external files. For
             * instance, if the data source is a DBMS, the concurrency controls
             * it enforces on its own data can be applied to the external file
             * as well.
             *
             * A DATALINK value is retrieved from a ResultSet object with the
             * ResultSet methods getURL or getObject. If the Java platform does
             * not support the type of URL returned by getURL or getObject, a
             * DATALINK value can be retrieved as a String object with the
             * method getString.
             *
             * java.net.URL values are stored in a database using the method
             * setURL. If the Java platform does not support the type of URL
             * being set, the method setString can be used instead.
             *
             *
             */
            case Types.DATALINK:
                return result.getURL(i);
            /*
             * The JDBC DATE type represents a date consisting of day, month,
             * and year. The corresponding SQL DATE type is defined in SQL-92,
             * but it is implemented by only a subset of the major databases.
             * Some databases offer alternative SQL types that support similar
             * semantics.
             */
            case Types.DATE:
                try {
                    java.sql.Date d = result.getDate(i, calendar);
                    return d != null ? formatDate(d.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            case Types.TIME:
                try {
                    Time t = result.getTime(i, calendar);
                    return t != null ? formatDate(t.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            case Types.TIMESTAMP:
                try {
                    Timestamp t = result.getTimestamp(i, calendar);
                    return t != null ? formatDate(t.getTime()) : null;
                } catch (SQLException e) {
                    // java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column ... to TIMESTAMP.
                    return null;
                }
            /*
             * The JDBC types DECIMAL and NUMERIC are very similar. They both
             * represent fixed-precision decimal values.
             *
             * The corresponding SQL types DECIMAL and NUMERIC are defined in
             * SQL-92 and are very widely implemented. These SQL types take
             * precision and scale parameters. The precision is the total number
             * of decimal digits supported, and the scale is the number of
             * decimal digits after the decimal point. For most DBMSs, the scale
             * is less than or equal to the precision. So for example, the value
             * "12.345" has a precision of 5 and a scale of 3, and the value
             * ".11" has a precision of 2 and a scale of 2. JDBC requires that
             * all DECIMAL and NUMERIC types support both a precision and a
             * scale of at least 15.
             *
             * The sole distinction between DECIMAL and NUMERIC is that the
             * SQL-92 specification requires that NUMERIC types be represented
             * with exactly the specified precision, whereas for DECIMAL types,
             * it allows an implementation to add additional precision beyond
             * that specified when the type was created. Thus a column created
             * with type NUMERIC(12,4) will always be represented with exactly
             * 12 digits, whereas a column created with type DECIMAL(12,4) might
             * be represented by some larger number of digits.
             *
             * The recommended Java mapping for the DECIMAL and NUMERIC types is
             * java.math.BigDecimal. The java.math.BigDecimal type provides math
             * operations to allow BigDecimal types to be added, subtracted,
             * multiplied, and divided with other BigDecimal types, with integer
             * types, and with floating point types.
             *
             * The method recommended for retrieving DECIMAL and NUMERIC values
             * is ResultSet.getBigDecimal. JDBC also allows access to these SQL
             * types as simple Strings or arrays of char. Thus, Java programmers
             * can use getString to receive a DECIMAL or NUMERIC result.
             * However, this makes the common case where DECIMAL or NUMERIC are
             * used for currency values rather awkward, since it means that
             * application writers have to perform math on strings. It is also
             * possible to retrieve these SQL types as any of the Java numeric
             * types.
             */
            case Types.DECIMAL:
            case Types.NUMERIC:
                BigDecimal bd = null;
                try {
                    // getBigDecimal() should get obsolete. Most seem to use getString/getObject anyway...
                    bd = result.getBigDecimal(i);
                } catch (NullPointerException e) {
                    // But is it true? JDBC NPE exists since 13 years?
                    // http://forums.codeguru.com/archive/index.php/t-32443.html
                    // Null values are driving us nuts in JDBC:
                    // http://stackoverflow.com/questions/2777214/when-accessing-resultsets-in-jdbc-is-there-an-elegant-way-to-distinguish-betwee
                }
                if (bd == null || result.wasNull()) {
                    return null;
                }
                if (scale >= 0) {
                    bd = bd.setScale(scale, rounding);
                    try {
                        long l = bd.longValueExact();
                        if (Long.toString(l).equals(result.getString(i))) {
                            // convert to long if possible
                            return l;
                        } else {
                            // convert to double (with precision loss)
                            return bd.doubleValue();
                        }
                    } catch (ArithmeticException e) {
                        return bd.doubleValue();
                    }
                } else {
                    return bd.toPlainString();
                }
            /*
             * The JDBC type DOUBLE represents a "double precision" floating
             * point number that supports 15 digits of mantissa.
             *
             * The corresponding SQL type is DOUBLE PRECISION, which is defined
             * in SQL-92 and is widely supported by the major databases. The
             * SQL-92 standard leaves the precision of DOUBLE PRECISION up to
             * the implementation, but in practice all the major databases
             * supporting DOUBLE PRECISION support a mantissa precision of at
             * least 15 digits.
             *
             * The recommended Java mapping for the DOUBLE type is as a Java
             * double.
             */
            case Types.DOUBLE:
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                return NumberFormat.getInstance(locale).parse(s).doubleValue();
            /*
             * The JDBC type FLOAT is basically equivalent to the JDBC type
             * DOUBLE. We provided both FLOAT and DOUBLE in a possibly misguided
             * attempt at consistency with previous database APIs. FLOAT
             * represents a "double precision" floating point number that
             * supports 15 digits of mantissa.
             *
             * The corresponding SQL type FLOAT is defined in SQL-92. The SQL-92
             * standard leaves the precision of FLOAT up to the implementation,
             * but in practice all the major databases supporting FLOAT support
             * a mantissa precision of at least 15 digits.
             *
             * The recommended Java mapping for the FLOAT type is as a Java
             * double. However, because of the potential confusion between the
             * double precision SQL FLOAT and the single precision Java float,
             * we recommend that JDBC programmers should normally use the JDBC
             * DOUBLE type in preference to FLOAT.
             */
            case Types.FLOAT:
                s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            /*
             * The JDBC type JAVA_OBJECT, added in the JDBC 2.0 core API, makes
             * it easier to use objects in the Java programming language as
             * values in a database. JAVA_OBJECT is simply a type code for an
             * instance of a class defined in the Java programming language that
             * is stored as a database object. The type JAVA_OBJECT is used by a
             * database whose type system has been extended so that it can store
             * Java objects directly. The JAVA_OBJECT value may be stored as a
             * serialized Java object, or it may be stored in some
             * vendor-specific format.
             *
             * The type JAVA_OBJECT is one of the possible values for the column
             * DATA_TYPE in the ResultSet objects returned by various
             * DatabaseMetaData methods, including getTypeInfo, getColumns, and
             * getUDTs. The method getUDTs, part of the new JDBC 2.0 core API,
             * will return information about the Java objects contained in a
             * particular schema when it is given the appropriate parameters.
             * Having this information available facilitates using a Java class
             * as a database type.
             */
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                return result.getObject(i);
            /*
             * The JDBC type REAL represents a "single precision" floating point
             * number that supports seven digits of mantissa.
             *
             * The corresponding SQL type REAL is defined in SQL-92 and is
             * widely, though not universally, supported by the major databases.
             * The SQL-92 standard leaves the precision of REAL up to the
             * implementation, but in practice all the major databases
             * supporting REAL support a mantissa precision of at least seven
             * digits.
             *
             * The recommended Java mapping for the REAL type is as a Java
             * float.
             */
            case Types.REAL:
                s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                return  NumberFormat.getInstance(locale).parse(s).doubleValue();
            /*
             * The JDBC type TINYINT represents an 8-bit integer value between 0
             * and 255 that may be signed or unsigned.
             *
             * The corresponding SQL type, TINYINT, is currently supported by
             * only a subset of the major databases. Portable code may therefore
             * prefer to use the JDBC SMALLINT type, which is widely supported.
             *
             * The recommended Java mapping for the JDBC TINYINT type is as
             * either a Java byte or a Java short. The 8-bit Java byte type
             * represents a signed value from -128 to 127, so it may not always
             * be appropriate for larger TINYINT values, whereas the 16-bit Java
             * short will always be able to hold all TINYINT values.
             */
            /*
             * The JDBC type SMALLINT represents a 16-bit signed integer value
             * between -32768 and 32767.
             *
             * The corresponding SQL type, SMALLINT, is defined in SQL-92 and is
             * supported by all the major databases. The SQL-92 standard leaves
             * the precision of SMALLINT up to the implementation, but in
             * practice, all the major databases support at least 16 bits.
             *
             * The recommended Java mapping for the JDBC SMALLINT type is as a
             * Java short.
             */
            /*
             * The JDBC type INTEGER represents a 32-bit signed integer value
             * ranging between -2147483648 and 2147483647.
             *
             * The corresponding SQL type, INTEGER, is defined in SQL-92 and is
             * widely supported by all the major databases. The SQL-92 standard
             * leaves the precision of INTEGER up to the implementation, but in
             * practice all the major databases support at least 32 bits.
             *
             * The recommended Java mapping for the INTEGER type is as a Java
             * int.
             */
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                try {
                    Integer integer = result.getInt(i);
                    return result.wasNull() ? null : integer;
                } catch (SQLDataException e) {
                    Long l = result.getLong(i);
                    return result.wasNull() ? null : l;
                }
            case Types.SQLXML:
                SQLXML xml = result.getSQLXML(i);
                return xml != null ? xml.getString() : null;
            case Types.NULL:
                return null;
            /*
             * The JDBC type DISTINCT field (Types class)>DISTINCT represents
             * the SQL3 type DISTINCT.
             *
             * The standard mapping for a DISTINCT type is to the Java type to
             * which the base type of a DISTINCT object would be mapped. For
             * example, a DISTINCT type based on a CHAR would be mapped to a
             * String object, and a DISTINCT type based on an SQL INTEGER would
             * be mapped to an int.
             *
             * The DISTINCT type may optionally have a custom mapping to a class
             * in the Java programming language. A custom mapping consists of a
             * class that implements the interface SQLData and an entry in a
             * java.util.Map object.
             */
            case Types.DISTINCT:
            /*
             * The JDBC type STRUCT represents the SQL99 structured type. An SQL
             * structured type, which is defined by a user with a CREATE TYPE
             * statement, consists of one or more attributes. These attributes
             * may be any SQL data type, built-in or user-defined.
             *
             * The standard mapping for the SQL type STRUCT is to a Struct
             * object in the Java programming language. A Struct object contains
             * a value for each attribute of the STRUCT value it represents.
             *
             * A STRUCT value may optionally be custom mapped to a class in the
             * Java programming language, and each attribute in the STRUCT may
             * be mapped to a field in the class. A custom mapping consists of a
             * class that implements the interface SQLData and an entry in a
             * java.util.Map object.
             *
             *
             */
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
                logger.log(Level.WARNING, "JDBC type not implemented: {}", type);
                return null;
            default: {
                logger.log(Level.WARNING, "unknown JDBC type ignored: {}", type);
                return null;
            }
        }
        return null;
    }

    public int toJDBCType(String type) {
        if (type == null) {
            return Types.NULL;
        } else if (type.equalsIgnoreCase("NULL")) {
            return Types.NULL;
        } else if (type.equalsIgnoreCase("TINYINT")) {
            return Types.TINYINT;
        } else if (type.equalsIgnoreCase("SMALLINT")) {
            return Types.SMALLINT;
        } else if (type.equalsIgnoreCase("INTEGER")) {
            return Types.INTEGER;
        } else if (type.equalsIgnoreCase("BIGINT")) {
            return Types.BIGINT;
        } else if (type.equalsIgnoreCase("REAL")) {
            return Types.REAL;
        } else if (type.equalsIgnoreCase("FLOAT")) {
            return Types.FLOAT;
        } else if (type.equalsIgnoreCase("DOUBLE")) {
            return Types.DOUBLE;
        } else if (type.equalsIgnoreCase("DECIMAL")) {
            return Types.DECIMAL;
        } else if (type.equalsIgnoreCase("NUMERIC")) {
            return Types.NUMERIC;
        } else if (type.equalsIgnoreCase("BIT")) {
            return Types.BIT;
        } else if (type.equalsIgnoreCase("BOOLEAN")) {
            return Types.BOOLEAN;
        } else if (type.equalsIgnoreCase("BINARY")) {
            return Types.BINARY;
        } else if (type.equalsIgnoreCase("VARBINARY")) {
            return Types.VARBINARY;
        } else if (type.equalsIgnoreCase("LONGVARBINARY")) {
            return Types.LONGVARBINARY;
        } else if (type.equalsIgnoreCase("CHAR")) {
            return Types.CHAR;
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            return Types.VARCHAR;
        } else if (type.equalsIgnoreCase("LONGVARCHAR")) {
            return Types.LONGVARCHAR;
        } else if (type.equalsIgnoreCase("DATE")) {
            return Types.DATE;
        } else if (type.equalsIgnoreCase("TIME")) {
            return Types.TIME;
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            return Types.TIMESTAMP;
        } else if (type.equalsIgnoreCase("CLOB")) {
            return Types.CLOB;
        } else if (type.equalsIgnoreCase("BLOB")) {
            return Types.BLOB;
        } else if (type.equalsIgnoreCase("ARRAY")) {
            return Types.ARRAY;
        } else if (type.equalsIgnoreCase("STRUCT")) {
            return Types.STRUCT;
        } else if (type.equalsIgnoreCase("REF")) {
            return Types.REF;
        } else if (type.equalsIgnoreCase("DATALINK")) {
            return Types.DATALINK;
        } else if (type.equalsIgnoreCase("DISTINCT")) {
            return Types.DISTINCT;
        } else if (type.equalsIgnoreCase("JAVA_OBJECT")) {
            return Types.JAVA_OBJECT;
        } else if (type.equalsIgnoreCase("SQLXML")) {
            return Types.SQLXML;
        } else if (type.equalsIgnoreCase("ROWID")) {
            return Types.ROWID;
        }
        return Types.OTHER;
    }

    private String formatDate(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

}
