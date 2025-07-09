import { fundDetailsVerificationStatus } from "../../businesslogicsprovider/redemptionprovider/funddetailsverificationstatus.provider.js";

export const fund_details_verification_status = async (req, res) => {
  try {
    let dataObject = {};
    let transaction_date = req.body.transaction_date;
    let fund = req.body.fund;
    const result = await fundDetailsVerificationStatus(transaction_date, fund);
    res.status(200).send(result);
  } catch (e) {
    // console.log("Error while fetching redemption verifcation status", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
