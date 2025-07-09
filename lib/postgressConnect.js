import pkg from "pg";
import { poolArray } from "../utils/amcuserslists.js";
const { Pool } = pkg;

export let obj = {};
export const connectionpuller = async () => {
  try {
    for (let ele = 0; ele < poolArray.length; ele++) {
      const pool = new Pool(poolArray[ele].config);
      pool.on("connect", () => {
        console.log(`Postgre Database Connected ${poolArray[ele].name}`);
      });
      pool.on("end", () => {
        console.log(`Database Disconnected ${poolArray[ele].name}`);
      });
      pool.on("error", (err) => {
        console.log(`Database Disconnected ${poolArray[ele].name}`, err);
      });
      await pool.connect();
      obj[poolArray[ele].fund] = pool;
    }
  } catch (e) {
    console.log("Error in databse connection", e);
  }
};

export const getPoolByFund = (fund) => {
  return obj[fund] || null;
};
