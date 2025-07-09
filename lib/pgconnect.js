import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  host: "10.248.15.177",
  database: 'lic',
  user: 'licuatappsvc',
  password: 'Reset123!',
  max: 35,
  connectionTimeoutMillis: 0,
  idleTimeoutMillis: 1000,
  port: "24999"
});

pool.on("connect", () => {
  console.log("Postgre Database Connected !!");
});

pool.on("end", () => {
  console.log("Database Disconnected !!");
});

export default pool;
