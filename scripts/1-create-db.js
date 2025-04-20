const { getDb } = require("../src/db");

async function createDatabase() {
  try {
    const db = await getDb();

    // Create orders collection
    await db.createCollection(process.env.ORDERS_COLLECTION);
    console.log(`Created collection: ${process.env.ORDERS_COLLECTION}`);

    // Create indexes for better query performance
    const ordersCollection = db.collection(process.env.ORDERS_COLLECTION);
    await ordersCollection.createIndex({ orderDate: 1 });
    await ordersCollection.createIndex({ customerId: 1 });
    await ordersCollection.createIndex({ status: 1 });

    console.log("Created indexes on orderDate, customerId, and status");
    console.log("Database and collection setup completed successfully");
  } catch (error) {
    console.error("Error creating database and collection:", error);
    process.exit(1);
  }
}

createDatabase().then(() => process.exit(0));
