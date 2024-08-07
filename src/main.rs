use std::fs::File;
use std::io;
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use csv::{Reader, ReaderBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use rusqlite::{Connection, params};

#[tokio::main]
async fn main() -> Result<()> {
    let dir_path = Path::new("src");
    let file_path = download_archive(dir_path).await?;
    convert_to_sql(&file_path)?;

    Ok(())
}

pub async fn download_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://api.nationalgrideso.com/dataset/88313ae5-94e4-4ddc-a790-593554d8c6b9/resource/f93d1835-75bc-43e5-84ad-12472b180a98/download/df_fuel_ckan.csv";
    let file_name = "GB_Generation_Mix.csv";
    let file_path = temp_dir.join(file_name);

    let bar = create_spinner("Downloading generation mix csv...".to_string());
    download_csv(url, file_path.clone()).await?;
    bar.finish_with_message("Generation mix csv downloaded");

    Ok(file_path)
}

/// Downloads the tarball from the specified URL and saves it to the specified file path.
pub async fn download_csv(url: &str, file_path: PathBuf) -> Result<()> {
    let response = reqwest::get(url).await.expect("Failed to download file");

    if response.status().is_success() {
        // Create a file to save the downloaded content
        let mut file = File::create(file_path)?;
        let mut content = Cursor::new(response.bytes().await?);

        // Write the content to the file
        std::io::copy(&mut content, &mut file)?;
    } else {
        println!("Failed to download file: {}", response.status());
    }

    Ok(())
}

fn convert_to_sql(csv_file: &PathBuf) -> Result<()> {


    // Open the CSV file
    let file = File::open(csv_file).map_err(|e| {
        println!("Could not open csv file {}: {}", csv_file.display(), e);
        e
    })?;
    let total_rows = count_csv_rows(csv_file.to_str().unwrap())?;

    let mut rdr = Reader::from_reader(file);


    // Connect to SQLite database (or create if it doesn't exist)
    let conn = Connection::open("generation-mix-national.db")?;

    // Create a table with columns matching CSV headers
    conn.execute(
        "CREATE TABLE IF NOT EXISTS energy_data (
            DATETIME TEXT PRIMARY KEY,
            GAS REAL, COAL REAL, NUCLEAR REAL, WIND REAL, HYDRO REAL,
            IMPORTS REAL, BIOMASS REAL, OTHER REAL, SOLAR REAL, STORAGE REAL,
            GENERATION REAL, CARBON_INTENSITY REAL, LOW_CARBON REAL, ZERO_CARBON REAL,
            RENEWABLE REAL, FOSSIL REAL,
            GAS_perc REAL, COAL_perc REAL, NUCLEAR_perc REAL, WIND_perc REAL,
            HYDRO_perc REAL, IMPORTS_perc REAL, BIOMASS_perc REAL, OTHER_perc REAL,
            SOLAR_perc REAL, STORAGE_perc REAL, GENERATION_perc REAL,
            LOW_CARBON_perc REAL, ZERO_CARBON_perc REAL, RENEWABLE_perc REAL, FOSSIL_perc REAL
        )",
        [],
    )?;

    // Prepare the insert statement
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO energy_data (
            DATETIME, GAS, COAL, NUCLEAR, WIND, HYDRO, IMPORTS, BIOMASS, OTHER, SOLAR, STORAGE,
            GENERATION, CARBON_INTENSITY, LOW_CARBON, ZERO_CARBON, RENEWABLE, FOSSIL,
            GAS_perc, COAL_perc, NUCLEAR_perc, WIND_perc, HYDRO_perc, IMPORTS_perc, BIOMASS_perc,
            OTHER_perc, SOLAR_perc, STORAGE_perc, GENERATION_perc, LOW_CARBON_perc,
            ZERO_CARBON_perc, RENEWABLE_perc, FOSSIL_perc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )?;

    // Skip the header row
    let _cheaders = rdr.headers()?;
    let pb = create_progress_bar(total_rows as u64, "Creating sqlite file".to_string());

    // Iterate over CSV records and insert into database
    for (i, result) in rdr.records().enumerate() {
        let record = result?;
        if record.len() != 32 {
            println!("Warning: Row {} has {} columns, expected 32", i + 1, record.len());
            continue;
        }

        stmt.execute(params![
            &record[0], &record[1], &record[2], &record[3], &record[4], &record[5], &record[6],
            &record[7], &record[8], &record[9], &record[10], &record[11], &record[12], &record[13],
            &record[14], &record[15], &record[16], &record[17], &record[18], &record[19], &record[20],
            &record[21], &record[22], &record[23], &record[24], &record[25], &record[26], &record[27],
            &record[28], &record[29], &record[30], &record[31]
        ])?;
        pb.inc(1);
    }

    pb.finish_with_message("Created sqlite file");
    Ok(())
}
pub fn create_spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message);
    bar.enable_steady_tick(Duration::from_millis(100));

    bar
}
/// Creates a progress bar.
pub fn create_progress_bar(size: u64, message: String) -> ProgressBar {
    ProgressBar::new(size).with_message(message).with_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("##-"),
    )
}

fn count_csv_rows(file_path: &str) -> io::Result<usize> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut csv_reader = ReaderBuilder::new().from_reader(reader);

    let row_count = csv_reader.records().count();
    Ok(row_count)
}