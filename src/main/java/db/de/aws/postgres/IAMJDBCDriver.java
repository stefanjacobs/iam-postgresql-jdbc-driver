/**
  * MIT License
  *
  * Copyright (c) 2018 Rik Turnbull
  * Copyright (c) 2020 Stefan Jacobs
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package db.de.aws.postgres;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

// import software.amazon.awssdk.services.rds.RdsClientBuilder;
// import software.amazon.awssdk.services.rds.auth.GetIamAuthTokenRequest;
// import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;

import java.net.URI;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * IAM JDBC Driver wrapper for Postgres.
 *
 * @author Rik Turnbull (mysql)
 * @author Stefan Jacobs (postgresql)
 *
 */
public class IAMJDBCDriver implements java.sql.Driver {

  private final static String DRIVER_ALIAS = ":postgresqliam:";
  private final static String DRIVER_URL_PREFIX = "jdbc" + DRIVER_ALIAS;

  private final static String PROPERTY_AWS_REGION = "awsRegion";
  private final static String PROPERTY_PASSWORD = "password";
  private final static String PROPERTY_USER = "user";

  private final static String POSTGRES_DRIVER_ALIAS = ":postgresql:";
  private final static String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";

  static {
    try {
      DriverManager.registerDriver(new IAMJDBCDriver());
    } catch (Exception e) {
      throw new RuntimeException("Can't register driver!", e);
    }
  }

  private Driver _postgresqlDriver;

  /**
   * Creates a new {@link IAMJDBCDriver}.
   *
   * @throws ClassNotFoundException if the Postgres driver class is not found
   * @throws IllegalAccessException if the Postgres driver cannot be instantiated
   * @throws InstantiationException if the Postgres driver cannot be instantiated
   */
  public IAMJDBCDriver() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    _postgresqlDriver = (Driver) Class.forName(POSTGRES_DRIVER_CLASS).newInstance();
  }

  /**
   * Generates an RDS authentication token.
   *
   * @param region   the AWS region name
   * @param hostname the RDS instance hostname
   * @param port     the RDS instance port
   * @param username the RDS instance username
   */
  public static String generateAuthToken(String region, String hostname, String port, String username) {

    AwsCredentials credentials = DefaultCredentialsProvider.create().resolveCredentials();

    Aws4PresignerParams params = Aws4PresignerParams
      .builder()
      .expirationTime(Instant.now().plusSeconds(15 * 60))
      .awsCredentials(credentials)
      .signingName("rds-db")
      .signingRegion(Region.of(region))
      .build();
    
    SdkHttpFullRequest request = SdkHttpFullRequest
      .builder()
      .encodedPath("/")
      .host(hostname)
      .port(Integer.parseInt(port))
      .protocol("https")
      .method(SdkHttpMethod.GET)
      .appendRawQueryParameter("Action", "connect")
      .appendRawQueryParameter("DBUser", username)
      .build();
    
    return Aws4Signer.create().presign(request, params).getUri().toString().substring(8);
  }

  /**
   * {@inheritDoc}
   */
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.startsWith(DRIVER_URL_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  public Connection connect(String url, Properties properties) throws SQLException {
    if (!acceptsURL(url)) {
      throw new SQLException("Invalid url: '" + url + "'");
    }
    String postgresUrl = url.replace(DRIVER_ALIAS, POSTGRES_DRIVER_ALIAS);
    URI uri = URI.create(postgresUrl.substring(5));

    String password = generateAuthToken(properties.getProperty(PROPERTY_AWS_REGION), uri.getHost(),
        String.valueOf(uri.getPort()), getUsernameFromUriOrProperties(uri, properties));
    properties.setProperty(PROPERTY_PASSWORD, password);

    return _postgresqlDriver.connect(postgresUrl, properties);
  }

  /**
   * {@inheritDoc}
   */
  public int getMajorVersion() {
    return _postgresqlDriver.getMajorVersion();
  }

  /**
   * {@inheritDoc}
   */
  public int getMinorVersion() {
    return _postgresqlDriver.getMinorVersion();
  }

  /**
   * {@inheritDoc}
   */
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return _postgresqlDriver.getParentLogger();
  }

  /**
   * {@inheritDoc}
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
    DriverPropertyInfo[] info = _postgresqlDriver.getPropertyInfo(url, properties);
    if (info != null) {
      ArrayList<DriverPropertyInfo> infoList = new ArrayList<DriverPropertyInfo>(Arrays.asList(info));
      infoList.add(new DriverPropertyInfo(PROPERTY_AWS_REGION, null));
      info = infoList.toArray(new DriverPropertyInfo[infoList.size()]);
    }
    return info;
  }

  /**
   * {@inheritDoc}
   */
  public boolean jdbcCompliant() {
    return _postgresqlDriver.jdbcCompliant();
  }

  /**
   * Fetches the username from the properties and if it is missing checks the jdbc
   * uri.
   *
   * @param uri        jdbc uri
   * @param properties jdbc properties
   * @returns the username
   */
  private String getUsernameFromUriOrProperties(URI uri, Properties properties) {
    String username = properties.getProperty(PROPERTY_USER);

    if (username == null) {
      final String userInfo = uri.getUserInfo();
      if (userInfo != null) {
        username = userInfo.split(":")[0];
      }
    }

    return username;
  }
}
