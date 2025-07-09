import { swp_timeseries_provider } from "../../businesslogicsprovider/swpprovider/timeseries.provider.js";

export const timeseries_controller = async (req, res) => {
  try {
    const startDate = req.body.startDate;
    const endDate = req.body.endDate;
    const fund = req.body.fund;
    const data = await swp_timeseries_provider(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
