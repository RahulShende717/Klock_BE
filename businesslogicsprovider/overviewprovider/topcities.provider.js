import { getPoolByFund } from "../../lib/postgressConnect.js";
import OverViewMasterNew from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const handleTopCities = async (trdate, fundcode, snsstatus = false) => {
  try {
    const transaction_date = trdate;
    const fund = fundcode;
    let fund_schema = getFundWiseSchema(fund);
    const pool = getPoolByFund(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
    const keysToCheck = [
      "topcitiescombine",
      "topcitiesliquid",
      "topcitiesnonliquid",
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "overview" },
        {
          $or: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return isDataPresent.dataObject;
      c;
    } else {
      let dataObject = {};

      let query1 = `Select source,
	asset_class,
	sum(amount) as amount,
	sum(count_of_txn) as count_of_txn
from (
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_non_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and fund = '${fund}'
			group by 1,
				2
		)
		union
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and fund = '${fund}'
			group by 1,
				2
		)
	) p
group by 1,
	2
order by 3 desc
limit 15`;

      let query2 = `Select source,
	asset_class,
	sum(amount) as amount,
	sum(count_of_txn) as count_of_txn
from (
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_non_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and asset_class = 'LIQUID'
				and fund = '${fund}'
			group by 1,
				2
		)
		union
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and asset_class = 'LIQUID'
				and fund = '${fund}'
			group by 1,
				2
		)
	) p
group by 1,
	2
order by 3 desc
limit 15`;

      let query3 = `Select source,
	asset_class,
	sum(amount) as amount,
	sum(count_of_txn) as count_of_txn
from (
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_non_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and asset_class = 'NON LIQUID'
				and fund = '${fund}'
			group by 1,
				2
		)
		union
		(
			select distinct CASE
					WHEN (
						branch in (
							'WB99',
							'MB88',
							'MA88',
							'A888',
							'I888',
							'W888',
							'DT99'
						)
					) THEN 'AMC Digital Assets'
					WHEN (branch = 'MU99') THEN 'MFU Online'
					WHEN (
						branch in ('BS77', 'NS77', 'IX77', 'BS88', 'NS88', 'IX88')
					) THEN 'Exchange'
					WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
					WHEN (
						(branch LIKE '%99')
						AND (
							NOT (
								branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
							)
						)
					) THEN 'Channel Partner'
					WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
				END as source,
				asset_class,
				count(
					distinct(
						concat(
							coalesce(scheme, ''),
							coalesce(plan, ''),
							coalesce(branch, ''),
							coalesce(trno, ''),
							coalesce(folio, '')
						)
					)
				) as count_of_txn,
				sum(cast(cramt as decimal)) as amount
			from schema.final_batchclosed
			where substring(trdt, 1, 10) = '${transaction_date}'
				and asset_class = 'NON LIQUID'
				and fund = '${fund}'
			group by 1,
				2
		)
	) p
group by 1,
	2
order by 3 desc
limit 15`;

      const fetch_results = async (array) => {
        const all_promises = array.map((ele, ind) => {
          let allresponse = [];
          return new Promise(async (resolve, reject) => {
            let response_obj = {};
            const res = await pool.query(ele.query);
            response_obj = {
              name: ele.name,
              result: res.rows,
            };

            allresponse.push(response_obj);

            resolve(response_obj);
          });
        });
        return all_promises;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;

          if (item.name == "Top cities(nonliquid)") {
            let sortedByAmount = rows
              .slice()
              .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
            let sortedByCount = rows
              .slice()
              .sort(
                (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
              );

            dataObject.topcitiesnonliquid = {
              sortedByAmount: sortedByAmount,
              sortedByCount: sortedByCount,
            };
          } else if (item.name == "Top cities(liquid)") {
            let sortedByAmount = rows
              .slice()
              .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
            let sortedByCount = rows
              .slice()
              .sort(
                (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
              );

            dataObject.topcitiesliquid = {
              sortedByAmount: sortedByAmount,
              sortedByCount: sortedByCount,
            };
          } else if (item.name == "Top cities(combine)") {
            let sortedByAmount = rows
              .slice()
              .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
            let sortedByCount = rows
              .slice()
              .sort(
                (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
              );

            dataObject.topcitiescombine = {
              sortedByAmount: sortedByAmount,
              sortedByCount: sortedByCount,
            };
          }
        });
        return dataObject;
      };

      const main = async () => {
        const array = [
          {
            name: "Top cities(combine)",
            query: query1.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "Top cities(liquid)",
            query: query2.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "Top cities(nonliquid)",
            query: query3.replace(/schema/g, `${fund_schema}`),
          },
        ];

        const all_promises = await fetch_results(array);

        const all_result = await Promise.all(all_promises);

        const dataobject = await calculate_result(all_result);

        const cached_data = {
          topcitiescombine: {
            sortedByAmount: dataObject.topcitiescombine.sortedByAmount,
            sortedByCount: dataObject.topcitiescombine.sortedByCount,
          },
          topcitiesliquid: {
            sortedByAmount: dataObject.topcitiesliquid.sortedByAmount,
            sortedByCount: dataObject.topcitiesliquid.sortedByCount,
          },
          topcitiesnonliquid: {
            sortedByAmount: dataObject.topcitiesnonliquid.sortedByAmount,
            sortedByCount: dataObject.topcitiesnonliquid.sortedByCount,
          },
        };

        const cachedData = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "overview" },
          {
            $set: {
              date: transaction_date,
              "dataObject.topcitiescombine": cached_data.topcitiescombine,
              "dataObject.topcitiesliquid": cached_data.topcitiesliquid,
              "dataObject.topcitiesnonliquid": cached_data.topcitiesnonliquid,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return cachedData.dataObject;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    throw new Error(e);
  }
};
