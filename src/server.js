require("dotenv").config();
const express = require("express");
const { connectToDatabase, validateViews, getDb } = require("./db");
const {
  createMaterializedView,
} = require("../scripts/4-create-materialized-view");

const app = express();
app.use(express.json());

// Helper function to handle errors
function handleError(res, error, message) {
  console.error(message, error);
  res.status(500).json({ error: message });
}

// Query standard view
app.get("/api/analytics/standard", async (req, res) => {
  try {
    const db = await getDb();
    const query = {};

    // Apply filters if provided
    if (req.query.year) query["year"] = parseInt(req.query.year);
    if (req.query.month) query["month"] = parseInt(req.query.month);
    if (req.query.category) query["category"] = req.query.category;
    if (req.query.city) query["city"] = req.query.city;

    const startTime = Date.now();
    const results = await db
      .collection(process.env.STANDARD_VIEW_NAME)
      .find(query)
      .limit(parseInt(req.query.limit) || 100)
      .toArray();
    const queryTime = Date.now() - startTime;

    res.json({
      metadata: {
        queryTimeMs: queryTime,
        resultCount: results.length,
        viewType: "standard",
      },
      results,
    });
  } catch (error) {
    handleError(res, error, "Error querying standard view");
  }
});

// Query materialized view
app.get("/api/analytics/materialized", async (req, res) => {
  try {
    const db = await getDb();
    const query = {};

    // Apply filters if provided
    if (req.query.year) query["year"] = parseInt(req.query.year);
    if (req.query.month) query["month"] = parseInt(req.query.month);
    if (req.query.category) query["category"] = req.query.category;
    if (req.query.city) query["city"] = req.query.city;

    const startTime = Date.now();
    const results = await db
      .collection(process.env.MATERIALIZED_VIEW_NAME)
      .find(query)
      .limit(parseInt(req.query.limit) || 100)
      .toArray();
    const queryTime = Date.now() - startTime;

    res.json({
      metadata: {
        queryTimeMs: queryTime,
        resultCount: results.length,
        viewType: "materialized",
        lastUpdated: results[0]?.lastUpdated,
      },
      results,
    });
  } catch (error) {
    handleError(res, error, "Error querying materialized view");
  }
});

// Compare view performance
app.get("/api/analytics/compare", async (req, res) => {
  try {
    const db = await getDb();
    const query = {};

    // Apply filters if provided
    if (req.query.year) query["year"] = parseInt(req.query.year);
    if (req.query.month) query["month"] = parseInt(req.query.month);
    if (req.query.category) query["category"] = req.query.category;
    if (req.query.city) query["city"] = req.query.city;

    // Query both views and measure performance
    const standardStart = Date.now();
    const standardResults = await db
      .collection(process.env.STANDARD_VIEW_NAME)
      .find(query)
      .limit(parseInt(req.query.limit) || 100)
      .toArray();
    const standardTime = Date.now() - standardStart;

    const materializedStart = Date.now();
    const materializedResults = await db
      .collection(process.env.MATERIALIZED_VIEW_NAME)
      .find(query)
      .limit(parseInt(req.query.limit) || 100)
      .toArray();
    const materializedTime = Date.now() - materializedStart;

    res.json({
      performance: {
        standardView: {
          queryTimeMs: standardTime,
          resultCount: standardResults.length,
        },
        materializedView: {
          queryTimeMs: materializedTime,
          resultCount: materializedResults.length,
          lastUpdated: materializedResults[0]?.lastUpdated,
        },
        comparison: {
          timeDifferenceMs: standardTime - materializedTime,
          percentageDifference:
            (((standardTime - materializedTime) / standardTime) * 100).toFixed(
              2
            ) + "%",
        },
      },
      results: {
        standard: standardResults,
        materialized: materializedResults,
      },
    });
  } catch (error) {
    handleError(res, error, "Error comparing views");
  }
});

const PORT = process.env.PORT || 3000;
let client;

// Initialize database connection
async function initializeDatabase() {
  try {
    client = await connectToDatabase();
    console.log("Database connection initialized");
  } catch (error) {
    console.error("Failed to initialize database connection:", error);
    process.exit(1);
  }
}

// Graceful shutdown handler
function handleShutdown() {
  console.log("\nReceived shutdown signal. Closing database connection...");
  if (client) {
    client
      .close()
      .then(() => {
        console.log("Database connection closed.");
        process.exit(0);
      })
      .catch((error) => {
        console.error("Error closing database connection:", error);
        process.exit(1);
      });
  } else {
    process.exit(0);
  }
}

// Register shutdown handlers
process.on("SIGINT", handleShutdown);
process.on("SIGTERM", handleShutdown);

// Initialize database before starting server
initializeDatabase().then(async () => {
  const db = await getDb();

  // Views must be there (run scripts first)
  await validateViews(db);

  // Watch the orders collection to refresh the materialized view if needed
  await createMaterializedView();

  app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });
});
