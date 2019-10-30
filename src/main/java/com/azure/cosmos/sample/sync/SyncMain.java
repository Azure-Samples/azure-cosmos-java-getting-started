// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.sync;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosContainerProperties;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.CosmosItemRequestOptions;
import com.azure.cosmos.CosmosItemResponse;
import com.azure.cosmos.FeedOptions;
import com.azure.cosmos.FeedResponse;
import com.azure.cosmos.Resource;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SyncMain {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyCollection";

    private CosmosDatabase database;
    private CosmosContainer container;

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    public static void main(String[] args) {
        SyncMain p = new SyncMain();

        try {
            p.getStartedDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set this to their CosmosDB Account region
        defaultPolicy.setPreferredLocations(Lists.newArrayList("West US"));

        //  Create sync client
        client = new CosmosClientBuilder()
            .setEndpoint(AccountSettings.HOST)
            .setKey(AccountSettings.MASTER_KEY)
            .setConnectionPolicy(defaultPolicy)
            .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
            .buildClient();

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Setup family documents to create
        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getAndersenFamilyDocument());
        familiesToCreate.add(Families.getWakefieldFamilyDocument());
        familiesToCreate.add(Families.getJohnsonFamilyDocument());
        familiesToCreate.add(Families.getSmithFamilyDocument());

        createFamilies(familiesToCreate);

        System.out.println("Querying documents.");
        queryDocuments();
    }

    private void createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        database = client.createDatabaseIfNotExists(databaseName).getDatabase();

        System.out.println("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        System.out.println("Create collection " + containerName + " if not exists.");

        //  Create collection if not exists
        CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerName, "/lastName");

        //  Create container with 400 RU/s
        container = database.createContainerIfNotExists(containerProperties, 400).getContainer();

        System.out.println("Checking collection " + container.getId() + " completed!\n");
    }

    private void createFamilies(List<Family> families) throws Exception {
        double totalRequestCharge = 0;
        for (Family family : families) {

            //  Create item using container that we created using sync client

            //  Use lastName as partitionKey for cosmos item
            //  Using appropriate partition key improves the performance of database operations
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions(family.getLastName());
            CosmosItemResponse item = container.createItem(family, cosmosItemRequestOptions);

            //  Get request charge and other properties like latency, and diagnostics strings, etc.
            System.out.println(String.format("Created document with request charge of %.2f within" +
                    " duration %s",
                item.getRequestCharge(), item.getRequestLatency()));
            totalRequestCharge += item.getRequestCharge();
        }
        System.out.println(String.format("Created %d documents with total request " +
                "charge of %.2f",
            families.size(),
            totalRequestCharge));
    }

    private void queryDocuments() {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.maxItemCount(10);
        queryOptions.setEnableCrossPartitionQuery(true);
        //  Set populate query metrics to get metrics around query executions
        queryOptions.populateQueryMetrics(true);

        Iterator<FeedResponse<CosmosItemProperties>> feedResponseIterator = container.queryItems(
            "SELECT * FROM Family WHERE Family.lastName != 'Andersen'", queryOptions);

        feedResponseIterator.forEachRemaining(cosmosItemPropertiesFeedResponse -> {
            System.out.println("Got a page of query result with " +
                cosmosItemPropertiesFeedResponse.getResults().size() + " document(s)"
                + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            System.out.println("Document Ids " + cosmosItemPropertiesFeedResponse
                .getResults()
                .stream()
                .map(Resource::getId)
                .collect(Collectors.toList()));
        });
    }
}
