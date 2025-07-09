import { fundDetailsVerificationStatus } from "../../businesslogicsprovider/redemptionprovider/funddetailsverificationstatus.provider.js";
import { nigoPlatformCategoryWise } from "../../businesslogicsprovider/redemptionprovider/nigoplatformcategorywisesummary.provider.js";

export const nigo_platform_category_wise = async (req, res) => {
  try {
    let dataObject = {};
    let transaction_date = req.body.transaction_date;
    let fund = req.body.fund;
    const result = await nigoPlatformCategoryWise(transaction_date, fund);
    res.status(200).send(result);
  } catch (e) {
    // console.log("Error while fecthing nigoplatform wise summary ", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
