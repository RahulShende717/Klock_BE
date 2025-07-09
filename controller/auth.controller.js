import express, { json, response } from "express";
// import { logger } from "../lib/loggerModule.js";
import axios from "axios";
const router = express.Router();

router.post("/login", async (req, res) => {
  try {
    const email = req.body.email;
    const password = req.body.password;

    const authParams = { email: email, password: password, appName: "klock" };
    const result = await axios.post(`${process.env["IDMURL"]}/api/appLogin`, {
      data: authParams,
    });

    if (result.data == null) {
      res.status(200).send({ authenticated: false, message: "User not Found" });
    } else if (result.data.authenticated === false) {
      const errormessage = result.data.message;
      res.status(200).send({ authenticated: false, message: errormessage });
    } else if (result.data.authenticated === true) {
      let user = result.data["data"];
      res.status(200).send({
        authenticated: true,
        token: user.token,
        expiredIN: user.passwordResetDays,
        funds: user.funds
      });
    }
  } catch (err) {
    console.log("error while login", err);
    // logger.error(`The requested url: ${req.originalUrl}`, err)
    res.status(200).send({
      success: false,
      authenticated: err.response.data.authenticated,
      message: err.response.data.message,
      forcedPasswordReset: err.response.data.forcedPasswordReset,
    });
  }
});

router.post("/verifyOtp", async (req, res) => {
  try {
    let otp = req.body.otp;
    let token = req.body.token;
    axios.defaults.headers.common["Authorization"] = `Bearer ${token}`;
    axios
      .post(`${process.env["IDMURL"]}/api/verifyLoginOtp`, {
        otp: otp,
      })
      .then((result) => {
        let user = result.data;
        if (result.data == null) {
          res
            .status(200)
            .send({ authenticated: false, message: "User not Found" });
        } else if (result.data.authenticated === false) {
          const errormessage = result.data.message;
          res.status(200).send({ authenticated: false, message: errormessage });
        } else if (result.data.authenticated === true) {
          let user = result.data["data"];
          let userDetails = {
            firstName: user.firstName,
            lastName: user.lastName,
            email: user.email,
            phNumber: user.phNumber,
            Funds: user.funds.length,
            token: user.token,
            userId: req.body.userId,
          };
          const funds = user.funds;

          // if (!isContainingFunds(user.funds)) res.status(200).send({ authenticated: false, message: "No funds allowed for your account" });
          // else
          res.status(200).send({
            authenticated: true,
            userDetails: userDetails,
            funds: user.funds,
          });
        }
      });
  } catch (err) {
    logger.error(`The requested url: ${req.originalUrl}`, err);
    res.status(200).send({ success: false });
  }
});

export { router as useloginRoute };
