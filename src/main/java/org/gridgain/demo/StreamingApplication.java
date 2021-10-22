/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gridgain.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.ssl.SslContextFactory;
import org.gridgain.grid.configuration.GridGainConfiguration;

public class StreamingApplication {
    /* Application default execution time. */
    private static final int DEFAULT_EXEC_TIME_MINS = 60;

    /* Ignite client instance. */
    private static Ignite ignite;

    /* Market ticker. */
    private static MarketTicker ticker;

    public static void main(String args[]) {
        long execTime = DEFAULT_EXEC_TIME_MINS;
        String address = "e76064f8-2997-42fc-9fa8-a0d0f62397a4.gridgain-nebula-test.com:47500";
        String login = "test";
        String password = "testing1";

        if (args != null) {
            for (String arg : args) {
                if (arg.startsWith("execTime")) {
                    execTime = Integer.parseInt(arg.split("=")[1]);
                } else if (arg.startsWith("address")) {
                    address = arg.split("=")[1];
                } else {
                    System.err.println("Unsupported parameter: " + arg);
                    return;
                }
            }
        }

        if (address == null) {
            System.err.println("ERROR: please provide an address: address=<your cluster address>");
        }

        System.out.println("Application execution time: " + execTime + " minutes");
        System.out.println("Cluster address to connect: " + address + " minutes");

        System.setProperty("IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED", "true");
        System.setProperty("IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK", "true");

        IgniteConfiguration cfg = new IgniteConfiguration()
                .setClientMode(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                    .setIpFinder(new TcpDiscoveryVmIpFinder()
 //                       .setAddresses(Collections.singleton("70592604-8554-4b18-82b6-8606c4d41551.gridgain-nebula-test.com:47500"))))
                        .setAddresses(Collections.singleton(address))))
                .setCommunicationSpi(new TcpCommunicationSpi()
                    .setForceClientToServerConnections(true))
                .setPluginConfigurations(new GridGainConfiguration()
                        .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(
                                new SecurityCredentials(login , password)))
                .setRollingUpdatesEnabled(true)
        );


// Then use the following snippet to connect the client.
// Replace the {login} and {password} with your cluster credentials.


        ignite = Ignition.start(cfg);

        createSchema(ignite);

        // Starting Market Ticker.
        ticker = new MarketTicker(ignite);
        ticker.start();

        // Shutting down the application in 'execTime' minutes.
        new Timer().schedule(new TimerTask() {
            @Override public void run() {
                System.out.println("The execution time is over. Shutting down the application...");

                ticker.stop();
                ignite.close();

                System.exit(0);
            }
        }, execTime * 60 * 1000);
    }

    private static void createSchema(Ignite ignite) {
        IgniteCache utilityCache = ignite.getOrCreateCache(new CacheConfiguration("utilityCache"));

        utilityCache.query(new SqlFieldsQuery(
            "DROP TABLE IF EXISTS Trade").setSchema("PUBLIC")).getAll();

        utilityCache.query(new SqlFieldsQuery(
            "DROP TABLE IF EXISTS Buyer").setSchema("PUBLIC")).getAll();

        utilityCache.query(new SqlFieldsQuery(
            "CREATE TABLE Trade (" +
                "id long," +
                "buyer_id int," +
                "symbol varchar," +
                "order_quantity int," +
                "bid_price double," +
                "trade_type varchar," +
                "order_date timestamp," +
                "PRIMARY KEY(id, buyer_id)) " +
                "WITH \"backups=1, atomicity=transactional, cache_name=Trade, " +
                "KEY_TYPE=org.gridgain.demo.TradeKey, VALUE_TYPE=org.gridgain.demo.Trade, affinity_key=buyer_id\"").
            setSchema("PUBLIC")).getAll();

        utilityCache.query(new SqlFieldsQuery(
            "CREATE TABLE Buyer (" +
                "id int PRIMARY KEY," +
                "first_name varchar," +
                "last_name varchar," +
                "age int," +
                "goverment_id varchar) " +
                "WITH \"backups=1, cache_name=Buyer\""
        ).setSchema("PUBLIC")).getAll();

        SqlFieldsQuery query = new SqlFieldsQuery(
            "INSERT INTO Buyer (id, first_name, last_name, age, goverment_id) VALUES (?,?,?,?,?)").
            setSchema("PUBLIC");

        utilityCache.query(query.setArgs(1, "John", "Smith", 45, "7bfjd73"));
        utilityCache.query(query.setArgs(2, "Arnold", "Mazer", 55, "unb23212"));
        utilityCache.query(query.setArgs(3, "Lara", "Croft", 35, "12338fb31"));
        utilityCache.query(query.setArgs(4, "Patrick", "Green", 42, "asbn233"));
        utilityCache.query(query.setArgs(5, "Anna", "Romanoff", 46, "klnnk3823"));
        utilityCache.query(query.setArgs(6, "Alfred", "Black", 55, "32345"));
    }
}
