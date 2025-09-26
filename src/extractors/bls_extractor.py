import requests
import json
import time
from datetime import datetime

class BLSExtractor:

    def __init__(self):
        
        # Base url
        self.base_url = 'https://api.bls.gov/publicAPI/v1/timeseries/data/'

        # Tracks number of requests as daily limit is 25
        self.request_count = 0
        self.max_requests = 25

        print("BLS Extractor Initialized")

    def fetch_employment_data(self, series_id, start_year, end_year):

        """
        Fetch employment data

        Parameters:
        series_id: BLS code for data
        start_year: beginning of the date range
        end_year: end of date range

        Returns JSON data
        """

        # Check request limit
        if self.request_count >= self.max_requests:
            print(f"Daily limit of {self.max_requests} requests reached")
            return None
        
        # v1 API limitation is max 10 years per request
        year_span = end_year - start_year
        if year_span > 10:
            print(f"v1 API limited to 10 years. Adjusting to {start_year}-{start_year+9}")
            end_year = start_year + 9

        # Prepare the request payload
        payload = {
            "seriesid": [series_id],
            "startyear": str(start_year),
            "endyear": str(end_year)
        }

        # Convert to JSON
        data = json.dumps(payload)

        # Headers
        headers = {'Content-type': 'application/json'}

        try:
            print(f"Fetching data for {series_id} ({start_year}-{end_year})...")
            print(f"  Request {self.request_count + 1} of {self.max_requests} daily limit")

            # Make POST request
            response = requests.post(
                self.base_url, 
                data=data, 
                headers=headers
            )

            # Increment request counter
            self.request_count += 1

            # Check if request was sucessful
            if response.status_code == 200:
                json_data = response.json()
                
                if json_data['status'] == 'REQUEST_SUCCEEDED':
                    # Return all series data
                    all_series = {}
                    for series in json_data['Results']['series']:
                        series_id = series['seriesID']
                        series_data = series['data']
                        all_series[series_id] = series_data
                        print(f" {series_id}: {len(series_data)} data points")
                    
                    return all_series
                else:
                    print(f" BLS error: {json_data.get('message', [])}")
                    return None
            else:
                print(f" HTTP error {response.status_code}")
                return None
    
        except Exception as e:
            # Catch any errors
            print(f"Error: {str(e)}")

    def fetch_all_sectors(self):
        
        # Fetch data for all major employment sectors
        
        # Dictionary mapping sector names to their BLS codes
        sectors = {
        'Total_Nonfarm': 'CES0000000001',
        'Mining_Logging': 'CES1000000001',
        'Construction': 'CES2000000001',
        'Manufacturing': 'CES3000000001',
        'Trade_Transport_Utilities': 'CES4000000001',
        'Information': 'CES5000000001',
        'Financial': 'CES5500000001',
        'Professional_Business': 'CES6000000001',
        'Education_Health': 'CES6500000001',
        'Leisure_Hospitality': 'CES7000000001',
        'Government': 'CES9000000001',
        }

        # Store results
        all_data = {}

        # Get current year for date range
        current_year = datetime.now().year

        print("\n" + "="*50)
        print("FETCHING EMPLOYMENT DATA")
        print("="*50 + "\n")

        # Fetch data for each sector
        for sector_name, series_id in sectors.items():
            # Get last 3 years
            data = self.fetch_employment_data(
                series_id,
                current_year-3,
                current_year
            )

            if data:
                all_data[sector_name] = data

                # Save raw data as backup
                self.save_raw_data(sector_name, data)

                time.sleep(1)

        print(f"\n Successfully fetched {len(all_data)} sectors")
        print(f" Requests used: {self.request_count} of {self.max_requests} daily limit")

        return all_data

    def save_raw_data(self, sector_name, data):

        # Save raw API response to file as backup

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/raw_{sector_name}_{timestamp}.json"

        # Save to file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

        print(f" Saved to {filename}")

    def test_single_series(self):

        print("\n TESTING BLS v1 API CONNECTIOn")
        print("-"*40)

        # Test with Total Nonfarm Employment
        test_series = 'CES0000000001'
        current_year = datetime.now().year

        # Get just 2 years of data for testing
        data = self.fetch_employment_data(
            test_series,
            current_year - 2,
            current_year
        )

        if data and len(data) > 0:
            # Data is a dictionary, so we need to get the series data
            series_data = data[test_series]

            if series_data and len(series_data) > 0:
                print(f"\nSUCCESS! Connection working!")
                print(f"Got {len(series_data)} data points")
                print(f"\nSample data:")
                
                # Now we can iterate over the actual data points
                for i, point in enumerate(series_data[:3]):
                    print(f"  {point['year']}-{point['period']}: {point['value']} thousand employees")
                return True
        
        else:
            print("\nFAILED: Could not get data")
            print("Check your internet connection and try again")
            return False
    
# Test the extractor
if __name__ == "__main__":
    # This block only runs if you execute this file directly
    
    print("="*60)
    print("BLS DATA EXTRACTOR")
    print("="*60)
    
    extractor = BLSExtractor()
    
    # Test with single series
    if extractor.test_single_series():
        print("\n✅ API connection verified!")
        print("\nYou can now run fetch_all_sectors() to get all employment data")
        
        # Uncomment to fetch all sectors:
        all_data = extractor.fetch_all_sectors()