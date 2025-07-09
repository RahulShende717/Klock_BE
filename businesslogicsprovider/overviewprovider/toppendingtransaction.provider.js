import { getPoolByFund } from "../../lib/postgressConnect.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const handle_top_pending_transaction = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    const transaction_date = trdate;
    const fund = fundcode;
    const pool = getPoolByFund(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
    let fund_schema = getFundWiseSchema(fund);
    const keysToCheck = [
      "topPendingTransactionNonLiquid",
      "topPendingTransactionLiquid",
      "topPendingTransactionCombined",
      "topPendingTransactionTillDateNonLiquid",
      "topPendingTransactionTillDateLiquid",
      "topPendingTransactionTillDateCombined",
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
    } else {
      let dataObject = {};

      const query1 = `select distinct a.*,
	concat(
		coalesce(b.app_fname, ''),
		coalesce(b.app_mname, ''),
		coalesce(b.app_lname, '')
	) as InvName,
	concat(a.branch, '-', upper(bm.bm_name)) as branch_name
from (
		Select distinct a.asset_class,
			a.fund,
			upper(a.scheme) as scheme,
			upper(a.plan) as plan,
			upper(a.branch) as branch,
			a.trno,
			a.trtype,
			a.folio,
			replace(a.ihno, '.0', '') as ihno,
			sum(cast(a.cramt as decimal)) as amount
		from schema.final_non_batchclosed a
		where substring(a.trdt, 1, 10) = '${transaction_date}'
			and a.fund = '${fund}'
			and a.asset_class = 'NON LIQUID'
			and a.active_flag = 'P'
		group by 1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9
		order by 10 desc
		limit 50
	) a
	left join schema.appl3_mirror b on a.fund = b.app_fund
	and a.folio = b.app_acno
	and b.app_fund = '${fund}'
	left join schema.branch_master_mirror bm on a.fund = bm.bm_fund
	and a.branch = bm.bm_branch
	and bm.bm_fund = '${fund}'
order by 10 desc`;

      const query2 = `select distinct a.*,
	concat(
		coalesce(b.app_fname, ''),
		coalesce(b.app_mname, ''),
		coalesce(b.app_lname, '')
	) as InvName,
	concat(a.branch, '-', upper(bm.bm_name)) as branch_name
from (
		Select distinct a.asset_class,
			a.fund,
			upper(a.scheme) as scheme,
			upper(a.plan) as plan,
			upper(a.branch) as branch,
			a.trno,
			a.trtype,
			a.folio,
			replace(a.ihno, '.0', '') as ihno,
			sum(cast(a.cramt as decimal)) as amount
		from schema.final_non_batchclosed a
		where substring(a.trdt, 1, 10) = '${transaction_date}'
			and a.fund = '${fund}'
			and a.asset_class = 'LIQUID'
			and a.active_flag = 'P'
		group by 1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9
		order by 10 desc
		limit 50
	) a
	left join schema.appl3_mirror b on a.fund = b.app_fund
	and a.folio = b.app_acno
	and b.app_fund = '${fund}'
	left join schema.branch_master_mirror bm on a.fund = bm.bm_fund
	and a.branch = bm.bm_branch
	and bm.bm_fund = '${fund}'
order by 10 desc`;

      const query3 = `select distinct a.*,
	concat(
		coalesce(b.app_fname, ''),
		coalesce(b.app_mname, ''),
		coalesce(b.app_lname, '')
	) as InvName,
	concat(a.branch, '-', upper(bm.bm_name)) as branch_name
from (
		Select distinct a.asset_class,
			a.fund,
			upper(a.scheme) as scheme,
			upper(a.plan) as plan,
			upper(a.branch) as branch,
			a.trno,
			a.trtype,
			a.folio,
			replace(a.ihno, '.0', '') as ihno,
			sum(cast(a.cramt as decimal)) as amount
		from schema.final_non_batchclosed a
		where substring(a.trdt, 1, 10) = '${transaction_date}'
			and a.fund = '${fund}'
			and a.active_flag = 'P'
		group by 1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9
		order by 10 desc
		limit 50
	) a
	left join schema.appl3_mirror b on a.fund = b.app_fund
	and a.folio = b.app_acno
	and b.app_fund = '${fund}'
	left join schema.branch_master_mirror bm on a.fund = bm.bm_fund
	and a.branch = bm.bm_branch
	and bm.bm_fund = '${fund}'
order by 10 desc`;

      const query4 = `SELECT DISTINCT a.*,
      CONCAT(COALESCE(b.app_fname, ''), COALESCE(b.app_mname, ''), COALESCE(b.app_lname, '')) AS InvName,
      CONCAT(a.branch, '-', bm.bm_name) AS branch_name
  FROM (
      SELECT a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, REPLACE(a.ihno, '.0', '') AS ihno, SUM(CAST(a.cramt AS DECIMAL)) AS amount
      FROM dbo.non_batchclosed_12_23_s a
      WHERE a.trdt <= '${transaction_date}' AND a.fund = '${fund}' AND a.asset_class = 'NON LIQUID'
      GROUP BY a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, a.ihno
      ORDER BY amount DESC
      LIMIT 50
  ) a
  LEFT JOIN dbo.appl3_partition b ON a.fund = b.app_fund AND a.folio = b.app_acno AND b.app_fund = '${fund}'
  LEFT JOIN dbo.branch_master_partition bm ON a.fund = bm.bm_fund AND a.branch = bm.bm_branch AND bm.bm_fund = '${fund}'
  ORDER BY amount DESC;`;

      const query5 = `SELECT DISTINCT a.*,
         CONCAT(COALESCE(b.app_fname, ''), COALESCE(b.app_mname, ''), COALESCE(b.app_lname, '')) AS InvName,
         CONCAT(a.branch, '-', bm.bm_name) AS branch_name
     FROM (
         SELECT a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, REPLACE(a.ihno, '.0', '') AS ihno, SUM(CAST(a.cramt AS DECIMAL)) AS amount
         FROM dbo.non_batchclosed_12_23_s a
         WHERE a.trdt <= '${transaction_date}' AND a.fund = '${fund}' AND a.asset_class = 'LIQUID' AND a.active_flag = 'P'
         GROUP BY a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, a.ihno
         ORDER BY amount DESC
         LIMIT 50
     ) a
     LEFT JOIN dbo.appl3_partition b ON a.fund = b.app_fund AND a.folio = b.app_acno AND b.app_fund = '${fund}'
     LEFT JOIN dbo.branch_master_partition bm ON a.fund = bm.bm_fund AND a.branch = bm.bm_branch AND bm.bm_fund = '${fund}'
     ORDER BY amount DESC;
     `;

      const query6 = `SELECT DISTINCT a.*,
         CONCAT(COALESCE(b.app_fname, ''), COALESCE(b.app_mname, ''), COALESCE(b.app_lname, '')) AS InvName,
         CONCAT(a.branch, '-', bm.bm_name) AS branch_name
     FROM (
         SELECT a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, REPLACE(a.ihno, '.0', '') AS ihno, SUM(CAST(a.cramt AS DECIMAL)) AS amount
         FROM dbo.non_batchclosed_12_23_s a
         WHERE a.trdt <= '${transaction_date}' AND a.fund = '${fund}' AND a.active_flag = 'P'
         GROUP BY a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, a.ihno
         ORDER BY amount DESC
         LIMIT 50
     ) a
     LEFT JOIN dbo.appl3_partition b ON a.fund = b.app_fund AND a.folio = b.app_acno AND b.app_fund = '${fund}'
     LEFT JOIN dbo.branch_master_partition bm ON a.fund = bm.bm_fund AND a.branch = bm.bm_branch AND bm.bm_fund = '${fund}'
     ORDER BY amount DESC;
     `;

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            const result = await pool.query(queryObj.query);
            return {
              name: queryObj.name,
              result: result.rows,
            };
          })
        );
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          if (item.name == "Top 50 Transactions(nonliquid)") {
            dataObject.topPendingTransactionNonLiquid = rows;
          } else if (item.name == "Top 50 Transactions(liquid)") {
            dataObject.topPendingTransactionLiquid = rows;
          } else if (item.name == "Top 50 Transactions(combine)") {
            dataObject.topPendingTransactionCombined = rows;
          } else if (item.name == "Top 50 Transactions(till Date)(nonliquid)") {
            dataObject.topPendingTransactionTillDateNonLiquid = rows;
          } else if (item.name == "Top 50 Transactions(till Date)(liquid)") {
            dataObject.topPendingTransactionTillDateLiquid = rows;
          } else if (item.name == "Top 50 Transactions(till Date)(combine)") {
            dataObject.topPendingTransactionTillDateCombined = rows;
          }
        });
        return dataObject;
      };

      const main = async () => {
        const array = [
          {
            name: "Top 50 Transactions(nonliquid)",
            query: query1.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "Top 50 Transactions(liquid)",
            query: query2.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "Top 50 Transactions(combine)",
            query: query3.replace(/schema/g, `${fund_schema}`),
          },
          // {
          //   name: "Top 50 Transactions(till Date)(nonliquid)",
          //   query: query4,
          // },
          // {
          //   name: "Top 50 Transactions(till Date)(liquid)",
          //   query: query5,
          // },
          // {
          //   name: "Top 50 Transactions(till Date)(combine)",
          //   query: query6,
          // },
        ];

        const all_results = await fetch_results(array);

        // const all_result = await Promise.all(all_promises);
        dataObject = await calculate_result(all_results);
        const cachedData = {
          topPendingTransactionNonLiquid:
            dataObject.topPendingTransactionNonLiquid,
          topPendingTransactionLiquid: dataObject.topPendingTransactionLiquid,
          topPendingTransactionCombined:
            dataObject.topPendingTransactionCombined,
          topPendingTransactionTillDateNonLiquid:
            dataObject.topPendingTransactionTillDateNonLiquid,
          topPendingTransactionTillDateLiquid:
            dataObject.topPendingTransactionTillDateLiquid,
          topPendingTransactionTillDateCombined:
            dataObject.topPendingTransactionTillDateCombined,
        };

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "overview" },
          {
            $set: {
              date: transaction_date,
              "dataObject.topPendingTransactionNonLiquid":
                cachedData.topPendingTransactionNonLiquid,
              "dataObject.topPendingTransactionLiquid":
                cachedData.topPendingTransactionLiquid,
              "dataObject.topPendingTransactionCombined":
                cachedData.topPendingTransactionCombined,
              "dataObject.topPendingTransactionTillDateNonLiquid":
                cachedData.topPendingTransactionTillDateNonLiquid,
              "dataObject.topPendingTransactionTillDateLiquid":
                cachedData.topPendingTransactionTillDateLiquid,
              "dataObject.topPendingTransactionTillDateCombined":
                cachedData.topPendingTransactionTillDateCombined,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cached_data.dataObject;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    throw new Error(e);
  }
};
