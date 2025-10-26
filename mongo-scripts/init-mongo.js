function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForPrimary(replicaSetConnection, replicaSetName) {
    console.log(`Waiting for replica set "${replicaSetName}" to elect a primary...`);
    let isPrimaryElected = false;
    while(!isPrimaryElected) {
        try {
            const status = await replicaSetConnection.adminCommand({ replSetGetStatus: 1 });
            isPrimaryElected = status.members.some(member => member.stateStr === "PRIMARY");
        } catch (e) {
            // Ignore errors while waiting for primary
        }
        if (!isPrimaryElected) {
            await sleep(1000);
        }
    }
    console.log(`Primary elected for replica set "${replicaSetName}".`);
}

async function main() {
    console.log("Starting sharding setup...");

    const configRsName = "mongo-config-rs";
    const shard1RsName = "mongo-shard-1-rs";
    const shard2RsName = "mongo-shard-2-rs";
    const dbName = "weather_db";
    const collectionName = "weather_data";

    try {
        // Wait for the cluster to be ready (optional, can add sleep if needed)
        // Add shards to the cluster
        console.log("Adding shards to the cluster...");
        await sh.addShard(`${shard1RsName}/mongo-shard-1-a:27017`);
        await sh.addShard(`${shard2RsName}/mongo-shard-2-a:27017`);

        // Enable sharding on the database and collection
    console.log(`Enabling sharding for database "${dbName}"...`);
        await sh.enableSharding(dbName);

    console.log(`Sharding collection "${dbName}.${collectionName}"...`);
        await sh.shardCollection(`${dbName}.${collectionName}`, {'StationId': 'hashed'});
        console.log("Sharding setup completed successfully.");

        console.log("Creating compound index");
        db.getSiblingDB(dbName).getCollection(collectionName).createIndex(
            { 'StationId': 1, 'ObservationTime': 1 }, { unique: true }
        );

        console.log("Unique index created successfully.");
        console.log("MongoDB sharding initialization completed!!");
    } catch (error) {
        console.error("Error during sharding setup:", error);
    }
}

main();