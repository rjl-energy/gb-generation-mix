# GB Generation Mix downloader

A rust utility to download the UK generation mix data from the National Grid ESO API
and convert it to a sqlite database.

Historic GB generation mix is from the 1st of Jan 2009 through to today.

Data points are either MW or %.

The database will be saved to the users 'home' directory as `generation-mix-national.sqlite` and can be used
in further processing.

The original data is available
at [https://www.nationalgrideso.com](https://www.nationalgrideso.com/data-portal/historic-generation-mix/historic_gb_generation_mix).
