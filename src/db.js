require("dotenv").config();
const { MongoClient } = require("mongodb");

let client = null;

async function connectToDatabase() {
  if (client) return client;

  try {
    client = await MongoClient.connect(process.env.MONGODB_URI);
    console.log("Connected to MongoDB successfully");
    return client;
  } catch (error) {
    console.error("Failed to connect to MongoDB:", error);
    process.exit(1);
  }
}

async function validateViews(db) {
  try {
    // Get list of all collections including views
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map((col) => col.name);

    // Check for standard view
    if (!collectionNames.includes(process.env.STANDARD_VIEW_NAME)) {
      throw new Error(
        `Standard view '${process.env.STANDARD_VIEW_NAME}' not found. Please run 'npm run create-standard-view' first.`
      );
    }

    // Check for materialized view
    if (!collectionNames.includes(process.env.MATERIALIZED_VIEW_NAME)) {
      throw new Error(
        `Materialized view '${process.env.MATERIALIZED_VIEW_NAME}' not found. Please run 'npm run create-materialized-view' first.`
      );
    }

    console.log("Database views validation successful");
  } catch (error) {
    console.error("Database views validation failed:", error.message);
    throw error;
  }
}

async function getDb() {
  const client = await connectToDatabase();
  const db = client.db(process.env.DATABASE_NAME);
  return db;
}

module.exports = {
  connectToDatabase,
  getDb,
  validateViews,
};
