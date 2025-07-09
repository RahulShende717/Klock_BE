import pool from "./pgconnect.js";
import { getPoolByFund } from "./postgressConnect.js";

console.log("Env varibale", process.env.BUILD);
export let connectionPool;

export const getPoolObj = (fund) => {
  try {
    if (process.env.BUILD == "uat") {
      connectionPool = pool;
    } else {
      connectionPool = getPoolByFund(fund);
    }
  } catch (e) {
    console.log("Error while export pool based on uat or prod", e);
  }
};
