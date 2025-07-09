import { timeseries_provider } from "../../businesslogicsprovider/redemptionprovider/timeseries.provider.js";

export const redemptionTimeSeriesData = async (req, res) => {
  try {
    const { fund, startDate, endDate } = req.body;
    const data = await timeseries_provider(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in timeseries graph redemption", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
