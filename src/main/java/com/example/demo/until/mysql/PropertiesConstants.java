package com.example.demo.until.mysql;

/**
 * @author :hao.li
 */
public class PropertiesConstants {

    /**
     * key
     */
    public static final String PROPERTIES_FILE_NAME = "/kafka-flink_test.properties";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

    public static final String TOPIC = "topic";

    public static final String GROUP_ID = "group.id";
    public static final String WINDOWS_SIZE= "dtc.windowSizeMillis";

    public static final String KEY_DESERIALIZER = "key.deserializer";

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    public static final String KAFKA_BROKERS = "kafka.bootstrap.servers";
    public static final String KAFKA_ZOOKEEEPER_CONNECT="kafka.zookeeper.connect";
    public static final String KAFKA_TOPIC="kafka.topic";
    public static final String KAFKA_GROUP_ID="group.id";
    public static final String HTXX_TOPIC="htxx.topic";
    public static final String FZXX_TOPIC="fzxx.topic";
    public static final String ZQGL_TOPIC="zqgl.topic";
    public static final String ZJZH_TOPIC="zjzh.topic";
    public static final String DRWT_TOPIC="drwt.topic";
    public static final String RZQYTZ_TOPIC="rzqytz.topic";
    public static final String ZCFZXX_TOPIC="zcfzxx.topic";

    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    //htxx
    public static final String UPDATE_HTXX_SQL = "update.htxx.sql";
    public static final String INSERT_HTXX_SQL = "insert.htxx.sql";
    public static final String DELETE_HTXX_SQL = "delete.htxx.sql";
    public static final String TRUNCATE_HTXX_SQL = "truncate.htxx.sql";
    //fzxx
    public static final String UPDATE_FZXX_SQL = "update.fzxx.sql";
    public static final String INSERT_FZXX_SQL = "insert.fzxx.sql";
    public static final String DELETE_FZXX_SQL = "delete.fzxx.sql";
    public static final String TRUNCATE_FZXX_SQL = "truncate.fzxx.sql";

    //zqgl
    public static final String UPDATE_ZQGL_SQL = "update.zqgl.sql";
    public static final String INSERT_ZQGL_SQL = "insert.zqgl.sql";
    public static final String DELETE_ZQGL_SQL = "delete.zqgl.sql";
    public static final String TRUNXATE_ZQGL_SQL = "truncate.zqgl.sql";
    //zjzh
    public static final String UPDATE_SQL = "update.sql";
    public static final String INSERT_SQL = "insert.sql";
    public static final String DELETE_SQL = "delete.sql";
    public static final String TRUNCATE_SQL = "truncate.sql";
    //drwt
    public static final String UPDATE_DRWT_SQL = "update.drwt.sql";
    public static final String INSERT_DRWT_SQL = "insert.drwt.sql";
    public static final String DELETE_DRWT_SQL = "delete.drwt.sql";
    public static final String TRUNCATE_DRWT_SQL = "truncate.drwt.sql";
    //qytz
    public static final String INSERT_QYTZ_SQL = "insert.qytz.sql";
    public static final String UPDATE_QYTZ_SQL = "insert.qytz.sql";
    public static final String DELETE_QYTZ_SQL = "delete.qytz.sql";
    public static final String TRUNCATE_QYTZ_SQL = "truncate.qytz.sql";
 //zcfzxx
    public static final String INSERT_ZCFZXX_SQL = "insert.zcfzxx.sql";
    public static final String UPDATE_ZCFZXX_SQL = "insert.zcfzxx.sql";
    public static final String DELETE_ZCFZXX_SQL = "insert.zcfzxx.sql";
    public static final String TRUNCATE_ZCFZXX_SQL = "truncate.zcfzxx.sql";

    /**
     * default value
     */
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";

    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";

    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";

    public static final String DEFAULT_KAFKA_GROUP_ID = "localgroup";

    public static final String DEFAULT_TOPIC="dtc";

    public static final String DEFAULT_USERNAME = "root";

    public static final String DEFAULT_PASSWORD = "123456";

    public static final String DEFAULT_DRIVER_NAME = "com.mysql.jdbc.Driver";

    public static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:3306/click_traffic?useUnicode=true&characterEncoding=UTF-8";



    /**
     * default value
     */
    //mysql
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_DRIVER = "tidb.driver";
    //oracle
    public static final String ORACLE_DATABASE = "oracle.database";
    public static final String ORACLE_HOST = "oracle.host";
    public static final String ORACLE_PASSWORD = "oracle.password";
    public static final String ORACLE_PORT = "oracle.port";
    public static final String ORACLE_USERNAME = "oracle.username";
    public static final String ORACLE_DRIVER = "oracle.driver";
    //tidb
    public static final String TIDB_DATABASE = "tidb.database";
    public static final String TIDB_HOST = "tidb.host";
    public static final String TIDB_PASSWORD = "tidb.password";
    public static final String TIDB_PORT = "tidb.port";
    public static final String TIDB_USERNAME = "tidb.username";
    public static final String TIDB_DRIVER = "tidb.driver";


}
