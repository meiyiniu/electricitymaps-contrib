const express = require('express');
const app = express();
const cors = require('cors');
const fs = require('fs');
const url = require('url');

const PORT = process.argv[2] || 8001;

const RENEWABLE = new Set(['biomass', 'hydro', 'solar', 'wind'])
const PROPRIETARY_CONST = 1810;

const PROVINCES_MAP = {
  "Newfoundland": "CA-NL-LB",
  "Labrador": "CA-NL-NF",
  "New Brunswick": "CA-NB",
  "Nova Scotia": "CA-NS",
  "Prince Edward Island": "CA-PE",
  "Quebec": "CA-QC",
  "Ontario": "CA-ON",
  "Manitoba": "CA-MB",
  "Saskatchewan": "CA-SK",
  "Alberta": "CA-AB",
  "British Columbia": "CA-BC",
  "Yukon Territory": "CA-YT",
  "Northwest Territories": "CA-NT",
  "Nunavut": "CA-NU",
  "Montana": "US-NW-BPAT",
}

app.use(cors());

const DEFAULT_ZONE_KEY = 'DE';
const POWER_API_URL = 'http://127.0.0.1:8000';
const DEFAULT_JSON = './public/v6/details/hourly/DE.json';

app.get('/v6/details/:aggregate/:zoneId', async (req, res, next) => {
  const { aggregate, zoneId } = req.params;
  console.log(zoneId);
  if (zoneId.substring(0, 2) == 'CA') {
    // modify the contents of this json
    // Should check if CA data already exists, then we dont need to read from this and copy.. i guess it dont matter tho
    let raw_data = fs.readFileSync(DEFAULT_JSON);
    let CA_data = JSON.parse(raw_data);
    const province = zoneId.substring(3, 5);
    console.log(`${POWER_API_URL}/province/${province}/production`);
    const response = await fetch(`${POWER_API_URL}/province/${province}/production`)
      .then(response => response.json())
      .then(data => {
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["production"] = data["production"] ? data["production"] : {};
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["capacity"] = data["capacity"] ? data["capacity"] : {};
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["source"] = data["source"] ? data["source"] : "";
        let total_production = 0;
        let renewable_production = 0;
        let nuclear_production = "production" != {} && "nuclear" in data["production"] ? data["production"]["nuclear"] : 0;
        for (const key in data["production"]) {
          total_production += data["production"][key];
          // console.log(`key: ${key}, value: ${data['production'][key]}`);
          if (RENEWABLE.has(key)) {
            // console.log('key is in renewable');
            renewable_production += data["production"][key];
          }
        };
        // console.log(`renewable_production: ${renewable_production}`);
        // console.log(`total_production: ${total_production}`);
        let renewable = renewable_production/total_production;
        // console.log(`renewable: ${renewable}`);
        let low_carbon_intensity = (renewable_production + nuclear_production)/total_production;
        let carbon_intensity = low_carbon_intensity * PROPRIETARY_CONST;

        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["totalProduction"] = total_production;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["renewableRatio"] = renewable != NaN ? renewable : 0;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["renewableRatioProduction"] = renewable != NaN ? renewable : 0;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["fossilFuelRatio"] = low_carbon_intensity != NaN ? 1 -low_carbon_intensity : 0;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["fossilFuelRatioProduction"] = low_carbon_intensity != NaN ? 1-low_carbon_intensity: 0;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["co2intensity"] = carbon_intensity != NaN ? carbon_intensity: 0;
        CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["co2intensityProduction"] = carbon_intensity!= NaN ? carbon_intensity : 0;
      })
      .catch(err => console.log(err));

    for (const key in CA_data["data"]["zoneStates"]) {
      CA_data["data"]["zoneStates"][key]["exchange"] = {};
      CA_data["data"]["zoneStates"][key]["zoneKey"] = zoneId;
    }

    await fetch(`${POWER_API_URL}/province/${province}/exchange`)
      .then(response => response.json())
      .then(data => {
        console.log('exchange data!');
        console.log(data);
        // CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["exchange"] = {};
        for (const key in data["flow"]) {
          const mapped_key = PROVINCES_MAP[key];
          CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["exchange"][mapped_key] = data["flow"][key];
        }
        console.log("UWU", CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["exchange"]);
        // gambatte.... why is it showing that german shit
        // can u check if this makes sense.... dude
        // CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["production"] = data["production"];
        // CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["capacity"] = data["capacity"];
        // CA_data["data"]["zoneStates"]["2022-12-05T08:00:00Z"]["source"] = data["source"];
      })
      .catch(err => console.log(err));

    fs.writeFileSync(`./public/v6/details/hourly/${zoneId}.json`, JSON.stringify(CA_data)); 
    console.log("RESPONSE: " + response);
  }

  // if file exists return it, otherwise redirect to DEFAULT file
  if (fs.existsSync(`./public/v6/details/${aggregate}/${zoneId}.json`)) {
    // file structure of project will return the correct file
    next();
  } else {
    res.redirect(`/v6/details/${aggregate}/${DEFAULT_ZONE_KEY}`);
  }
});

app.use(function (req, res, next) {
  // Get rid of query parameters so we can serve static files
  if (Object.entries(req.query).length !== 0) {
    res.redirect(url.parse(req.url).pathname);
  } else {
    // Log all requests to static files
    console.log(req.method, req.path);
    next();
  }
});

app.use(express.static('public', { extensions: ['json'] }));

const server = app.listen(PORT, () => {
  console.log('Started mockserver on port: ' + PORT);
});
