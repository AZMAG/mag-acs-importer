const XLSX = require("xlsx");
const axios = require("axios");
const fs = require("fs");

const {
  get5yrDataByTableId,
  get1yrDataByTableId,
  get5yrGeoTable,
  get1yrGeoTable,
  createJoinedViewByYear,
  writeToSql,
  checkTableExists,
} = require("./data");

const { year, excelFile, outputDatabase, chunkSize } = require("./config");

const workbook = XLSX.readFile(excelFile);
const sheet_name_list = workbook.SheetNames;
const xlData = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]])(
  // .filter((row) => row['Download'] === 'Y')
  // .filter((row) => row["Table ID"] === "C24030");
  async () => {
    // Import GEO Tables
    const geos5Yr = await get5yrGeoTable();
    const tblName5Yr = `${outputDatabase}.dbo.G${year}5YR`;
    await writeToSql(geos5Yr, {
      tblName: tblName5Yr,
      chunkSize,
      idColumn: "GEO_ID",
      overwrite: true,
    });
    const geos1Yr = await get1yrGeoTable();
    const tblName1Yr = `${outputDatabase}.dbo.G${year}1YR`;
    await writeToSql(geos1Yr, {
      tblName: tblName1Yr,
      chunkSize,
      idColumn: "GEO_ID",
      overwrite: true,
    });
    console.log("Geo Tables Loaded");
    console.log(tblName5Yr);
    console.log(tblName1Yr);

    for (let i = 0; i < xlData.length; i++) {
      const row = xlData[i];
      const tableId = row["Table ID"];
      console.log(`Loading table file (${i} / ${xlData.length}) -- ${tableId}`);
      if (tableId) {
        // const oneYrData = await get1yrDataByTableId(tableId);
        const tblName = `${outputDatabase}.dbo.${tableId}`;
        const tableExists = await checkTableExists(tblName);
        if (tableExists) {
          console.log(`Table ${tableId} already exists. Skipping.`);
        }
        const fiveYrData = await get5yrDataByTableId(tableId);
        console.log(
          `Table Loaded. Writing to db... (${fiveYrData.length} rows)`
        );
        const tbl1YrName = tblName + "_1yr";
        await writeToSql(fiveYrData, {
          tblName,
          chunkSize,
          idColumn: "GEO_ID",
          overwrite: true,
        });
        // await writeToSql(oneYrData, {
        //   tblName: tbl1YrName,
        //   chunkSize,
        //   idColumn: 'GEO_ID',
        //   overwrite: true,
        // });
        console.log(`${tableId} - written to database.`);
      }
    }
  }
)();
// await createJoinedViewByYear(xlData, '5');
// await createJoinedViewByYear(xlData, '1');
