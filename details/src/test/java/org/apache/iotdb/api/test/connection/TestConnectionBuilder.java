package org.apache.iotdb.api.test.connection;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.testng.annotations.Test;

import java.util.*;

public class TestConnectionBuilder {
    private Session session = null;

    private void testConnection_param(String key, String value) throws IoTDBConnectionException {
        String host = "127.0.0.1";
        int port = 6667;
        String username = "root";
        String password = "root";
        Boolean redirectionFLG = false;
        Boolean compressFLG = false;

        switch (key) {
            case "host":
                host = value;break;
            case "username":
                username = value;break;
            case "password":
                password = value;break;
            case "port":
                port = Integer.parseInt(value);break;
            case "redirectionFLG":
                redirectionFLG = Boolean.parseBoolean(value);break;
            case "compressFLG":
                compressFLG = Boolean.parseBoolean(value);break;
        }
        session = new Session.Builder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .enableRedirection(redirectionFLG)
                .build();
        session.open(compressFLG);
    }
    private void testConnectionCluster_param(String key, Object value) throws IoTDBConnectionException {
        List<String> nodeUrls = new ArrayList<>();
        nodeUrls.add("localhost:6667");
        String username = "root";
        String password = "root";
        Boolean redirectionFLG = false;
        Boolean compressFLG = false;

        switch (key) {
            case "nodeUrls":
                nodeUrls = (List<String>)value;break;
            case "username":
                username = value.toString();break;
            case "password":
                password = value.toString();break;
            case "redirectionFLG":
                redirectionFLG = Boolean.parseBoolean(value.toString());break;
            case "compressFLG":
                compressFLG = Boolean.parseBoolean(value.toString());break;
        }
        session = new Session.Builder()
                .nodeUrls(nodeUrls)
                .username(username)
                .password(password)
                .enableRedirection(redirectionFLG)
                .build();
        session.open(compressFLG);
    }

    private void testSessionPool_param(String key, Object value) {
        String host = "127.0.0.1";
        int port = 6667;
        String username = "root";
        String password = "root";
        Boolean redirectionFLG = false;

        switch (key) {
            case "host":
                host = value.toString();break;
            case "username":
                username = value.toString();break;
            case "password":
                password = value.toString();break;
            case "redirectionFLG":
                redirectionFLG = Boolean.parseBoolean(value.toString());break;
            case "nodeUrls":
                SessionPool sessionPool = new SessionPool.Builder()
                        .nodeUrls((List<String>) value)
                        .user(username)
                        .password(password)
                        .enableRedirection(redirectionFLG)
                        .build();
                break;
        }

        SessionPool sessionPool = new SessionPool.Builder()
                .host(host)
                .port(port)
                .user(username)
                .password(password)
                .enableRedirection(redirectionFLG)
                .build();

    }

    @Test(priority = 10, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_hostNull() throws IoTDBConnectionException {
        testConnection_param("host", null);
    }
    @Test(priority = 11, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_usernameNull() throws IoTDBConnectionException {
        testConnection_param("username", null);
    }
    @Test(priority = 12, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_passwordNull() throws IoTDBConnectionException {
        testConnection_param("password", null);
    }
    @Test(priority = 13, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_redirectNull() throws IoTDBConnectionException {
        testConnection_param("redirectionFLG", null);
    }
    @Test(priority = 14, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_compressNull() throws IoTDBConnectionException {
        testConnection_param("compressFLG", null);
    }
    @Test(priority = 15, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_nodeUrlsNullInList() throws IoTDBConnectionException {
        List<String> nodeUrls = new ArrayList<>(1);
        nodeUrls.add(null);
        testConnectionCluster_param("nodeUrls", nodeUrls);
    }
    @Test(priority = 20, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_hostEmpty() throws IoTDBConnectionException {
        testConnection_param("host", "");
    }
    @Test(priority = 11, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_usernameEmptyl() throws IoTDBConnectionException {
        testConnection_param("username", "");
    }
    @Test(priority = 12, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_passwordEmpty() throws IoTDBConnectionException {
        testConnection_param("password", "");
    }
    @Test(priority = 13, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_redirectEmpty() throws IoTDBConnectionException {
        testConnection_param("redirectionFLG", "");
    }
    @Test(priority = 14, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_compressEmpty() throws IoTDBConnectionException {
        testConnection_param("compressFLG", "");
    }
    @Test(priority = 15, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_nodeUrlsEmpty() throws IoTDBConnectionException {
        testConnectionCluster_param("nodeUrls", new ArrayList<>(0));
    }
    @Test(priority = 30, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_hostErr() throws IoTDBConnectionException {
        testConnection_param("host", "aaa");
    }
    @Test(priority = 31, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_usernameErr() throws IoTDBConnectionException {
        testConnection_param("username", "user");
    }
    @Test(priority = 32, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_passwordErr() throws IoTDBConnectionException {
        testConnection_param("password", "passwd");
    }
    @Test(priority = 33, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_redirectErr() throws IoTDBConnectionException {
        testConnection_param("redirectionFLG", "t");
    }
    @Test(priority = 34, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_compressErr() throws IoTDBConnectionException {
        testConnection_param("compressFLG", "4");
    }
    @Test(priority = 35, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_errPort() throws IoTDBConnectionException {
        testConnection_param("port", "0");
    }
    @Test(priority = 36, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_errPortNegative() throws IoTDBConnectionException {
        testConnection_param("port", "-1");
    }
    @Test(priority = 40, expectedExceptions = IoTDBConnectionException.class)
    public void testConnect_nodeUrlsErr() throws IoTDBConnectionException {
        List<String> nodeUrls = new ArrayList<>(1);
        nodeUrls.add("localhost");
        testConnectionCluster_param("nodeUrls", nodeUrls);
    }

    @Test(priority = 50, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_hostNull() {
        testSessionPool_param("host", null);
    }
    @Test(priority = 51, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_userNull() {
        testSessionPool_param("username", null);
    }
    @Test(priority = 52, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_passwordNull() {
        testSessionPool_param("password", null);
    }
    @Test(priority = 53, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_redirectionFLGNull() {
        testSessionPool_param("redirectionFLG", null);
    }
    @Test(priority = 54, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_nodeUrlsNull() {
        testSessionPool_param("nodeUrls", null);
    }

    @Test(priority = 60, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_hostEmpty() {
        testSessionPool_param("host", "");
    }
    @Test(priority = 61, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_userEmpty() {
        testSessionPool_param("username", "");
    }
    @Test(priority = 62, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_passwordEmpty() {
        testSessionPool_param("password", "");
    }
    @Test(priority = 63, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_redirectionFLGEmpty() {
        testSessionPool_param("redirectionFLG", "");
    }
    @Test(priority = 64, expectedExceptions = IoTDBConnectionException.class)
    public void testSessionPool_nodeUrlsEmpty() {
        testSessionPool_param("nodeUrls", new ArrayList<>());
    }


}
