const { year, excelFile, outputDatabase } = require('./config');
const axios = require('axios');
const Papa = require('papaparse');
const MagSQL = require('mag-node-sql');
const sql = new MagSQL();
const censusWebsiteRoot = `https://www2.census.gov/programs-surveys/acs/summary_file/${year}/table-based-SF/data/`;
const oneYrPrefix = '1YRData/acsdt1y';
const fiveYrPrefix = '5YRData/acsdt5y';

async function get1yrDataByTableId(tableId) {
  const oneYrUrl = `${censusWebsiteRoot}/${oneYrPrefix}${year}-${tableId.toLowerCase()}.dat`;
  const res = await axios.get(oneYrUrl);
  return processData(res);
}

async function get5yrDataByTableId(tableId) {
  const fiveYrUrl = `${censusWebsiteRoot}/${fiveYrPrefix}${year}-${tableId.toLowerCase()}.dat`;
  const res = await axios.get(fiveYrUrl);
  return processData(res);
}
async function writeToSql(data, options) {
  await sql.ArrayToSQLTable(data, options);
  return true;
}

async function downloadTableShells() {
  //I:\Data\ACS_2021\ACS2021_5yr\Documentation\ACS21_5yr_TableShells.xlsx
}

async function getGeoTableByYear(yr) {
  const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${year}/table-based-SF/documentation/Geos${year}${5}YR.txt`;
  const res = await axios.get(url);
  let { data } = Papa.parse(res.data, { header: true });
  data = data
    .filter((row) => row['GEO_ID'])
    .map((row) => {
      if (row['GEO_ID']) {
        row['GEOID'] = row['GEO_ID'];
        row['GEOID20'] = row['GEO_ID'].split('US')[1];
        return row;
      }
    });
  return data;
}

async function get5yrGeoTable() {
  return getGeoTableByYear('5');
}
async function get1yrGeoTable() {
  return getGeoTableByYear('1');
}

function processData(res) {
  const lines = res.data.split(`\n`);
  const headers = lines[0].split('|');

  let dataRows = [];
  for (let j = 1; j < lines.length; j++) {
    const rawLine = lines[j];
    const arrLine = rawLine.split('|');
    let lineData = {};

    headers.forEach((header, k) => {
      let value = arrLine[k];
      if (header !== 'GEO_ID') {
        value = Number(value);
      }
      lineData[header] = value;
    });

    Object.keys(lineData).forEach((key) => {
      if (key.includes('_E')) {
        // Remove table number from field names
        lineData[key.replace('_E', '')] = lineData[key];
        delete lineData[key];
      } else if (key.includes('_M')) {
        // Filter out margin of error fields.
        delete lineData[key];
      }
    });
    if (lineData['GEO_ID'] && lineData['GEO_ID'] !== '') {
      // lineData['GEOID10'] = lineData['GEO_ID'].split('US')[1];
      dataRows.push(lineData);
    }
  }
  return dataRows;
}

async function checkTableExists(tblName) {
  let res = await sql.RunQuery(
    `IF OBJECT_ID('${tblName}', 'U') IS NOT NULL select 1 as a ELSE SELECT 0`
  );
  let val = res.recordset[0]['a'];
  return val ? true : false;
}

async function getTableColumnNames(tableId) {
  const { recordset } = await sql.RunQuery(
    `Select top 0 * from ${outputDatabase}.dbo.${tableId}`
  );
  const columnNames = Object.keys(recordset.columns).filter(
    (col) => col !== 'GEO_ID'
  );
  return columnNames;
}

async function createJoinedViewByYear(tables, yr) {
  const geoTableName = `${outputDatabase}.dbo.G${year}${yr}YR`;

  let selectTerms = [];
  let joinTerms = [];

  tables = tables.filter((table) => table.DemViewer);

  for (let i = 0; i < tables.length; i++) {
    const table = tables[i];
    const tableId = table['Table ID'];

    const fullTableName = `${outputDatabase}.dbo.${tableId}`;

    const selectFields = await getTableColumnNames(tableId);
    selectTerms.push(` ${selectFields.join(', ')}`);
    joinTerms.push(
      `LEFT JOIN ${fullTableName} ON ${fullTableName}.GEO_ID = ${geoTableName}.GEO_ID`
    );
  }

  const useQuery = `USE ${outputDatabase};`;

  await sql.RunQuery(useQuery);

  await sql.RunQuery(`DROP VIEW IF EXISTS dbo.ACS_${yr}yr_Viewer`);

  const query = `
      CREATE VIEW dbo.ACS_${yr}yr_Viewer as
      SELECT ${geoTableName}.*, ${selectTerms}
      FROM ${geoTableName}
      ${joinTerms.join(' ')}
  `;
  await sql.RunQuery(query);
  return;
}

module.exports = {
  get5yrDataByTableId,
  get1yrDataByTableId,
  get5yrGeoTable,
  get1yrGeoTable,
  createJoinedViewByYear,
  writeToSql,
  checkTableExists,
};
