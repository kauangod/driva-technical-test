import express from "express";
const app = express();

app.get("/health", (_, res) => {
  res.json({ status: "ok" });
});

app.listen(3000, () => console.log("API up"));
