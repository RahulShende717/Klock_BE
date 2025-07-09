import { handleTopCities } from "../../businesslogicsprovider/overviewprovider/topcities.provider.js";

export const topcities_controller = async (req, res) => {
  try {
    const transaction_date = req.body.currentDate;
    const fund = req.body.fund;
    const data = await handleTopCities(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
