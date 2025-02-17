#[macro_use]
extern crate log;
extern crate env_logger as logger;

// Standard Library
use std::env;

// Ecternal Library
use chrono::{DateTime, Duration, Local};
use clap::Parser;
use serde_json::{Map, Value}; 

use json_ld_utils::{
    load_json_ld, scan_json_ld_obj, DBP_BASE_URL, DBP_BREWER_INFO, DBP_BREWER_OUTPUT_STORE,
    DBP_PATTERN, DBP_RWD_BREWING_DEMAND, DBP_TIME_PERIOD_END, DBP_TIME_PERIOD_START, SC_NAME
};
use utils::extract_data_sets;

mod protocols;
mod utils;

mod data_brewer_micro;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(
        short = 'j',
        long = "json_ld",
        value_name = "RealWorldDataset JSON-LD List API URL",
        default_value = "https://dev-rwdb.srv.exdata.co.jp/api/v0/brewing_demands/176/?format=json"
    )]
    json_ld: String,
    #[arg(
        short = 'l',
        long = "log_level",
        value_name = "Log Level (ERROR, INFO, DEBUG)",
        default_value = "INFO"
    )]
    log_level: String,
}

fn brewing_data_sample(
    pattern: &str,
    data_set_base_path: &str,
    brewing_arguments: &Vec<Map<String, Value>>,
    output_path: &str,
    dt_start: &DateTime<Local>,
    dt_end: &DateTime<Local>,
    duration: &Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    match pattern {
        "%Y/%Y-%m-%d.extention" => {
            match data_set_base_path {
                _ if data_set_base_path.starts_with(protocols::FILE) => { 
                    let file_path = data_set_base_path.to_string().replace(protocols::FILE, "");
                    let mut dt = *dt_start;
                    while dt <= *dt_end {
                            let year_str = dt.format("%Y").to_string();
                            let date_str = dt.format("%Y-%m-%d").to_string();
                            let data_set_path = format!("{}{}/{}.extention",file_path, year_str, date_str);
                            info!("data_set_path: {}", data_set_path);
                            let file = std::fs::File::open(&data_set_path).map_err(|e| {
                                eprintln!("Error opening file {}: {}", data_set_path, e);
                                Box::<dyn std::error::Error>::from("Error: Unable to open file")
                            })?;
                            let mut brewed_data: String = String::new();
                            for arg in brewing_arguments {
                                brewed_data = data_brewer_micro::data_brewer_sample(&file, arg.clone()).map_err(|e| {
                                    eprintln!("Error brewing data: {}", e);
                                    Box::<dyn std::error::Error>::from("Error: Unable to brew data")
                                })?;
                            }

                            info!("brewed_data: {:?}", brewed_data);
                            match output_path {
                                _ if output_path.starts_with(protocols::FILE) => {
                                    info!("output_path: {}", output_path);
                                    let output_file_path = format!("{}{}/{}.extention", output_path, year_str, date_str);
                                    utils::mkdir_to_dest(&output_file_path, *dt_start, *dt_end, *duration);
                                    let output_path = output_file_path.to_string().replace(protocols::FILE, "");
                                    std::fs::write(&output_path, brewed_data).map_err(|e| {
                                        eprintln!("Error writing file {}: {}", output_path, e);
                                        Box::<dyn std::error::Error>::from("Error: Unable to write file")
                                    })?;
                                }
                                // _ if  data_set_base_path.starts_with(protocols::FTP) => {},   // TODO: Implement this!
                                // _ if  data_set_base_path.starts_with(protocols::HTTP) => {},  // TODO: Implement this!
                                // _ if  data_set_base_path.starts_with(protocols::HTTPS) => {}, // TODO: Implement this!
                                _ => {
                                    eprintln!("Error: Unknown output_path protocol");
                                    return Err("Error: Unknown output_path protocol".into());
                                }
                            }

                            dt += *duration;
                        }
                        Ok(())
                    }
                // _ if  data_set_base_path.starts_with(protocols::FTP) => {},   // TODO: Implement this!
                // _ if  data_set_base_path.starts_with(protocols::HTTP) => {},  // TODO: Implement this!
                // _ if  data_set_base_path.starts_with(protocols::HTTPS) => {}, // TODO: Implement this!
                _ => {
                    eprintln!("Error: Unknown data_set_pattern");
                    return Err("Error: Unknown data_set_pattern".into());
                }
            }
        }
        _ => {
            eprintln!("Error: Unknown data_set_pattern");
            return Err("Error: Unknown data_set_pattern".into());
        }
    }
}



async fn process_demand(json_ld: &str) -> Result<(), Box<dyn std::error::Error>> {
    let loaded_json_ld: serde_json::Map<String, serde_json::Value> = if json_ld.starts_with("http") {
        load_json_ld(json_ld, 6, false).await
        .unwrap_or_else(|e| {
            eprintln!("Failed to load JSON-LD: {}", e);
            std::process::exit(1);
        })
    } else {
        let mut loaded_json_ld = serde_json::from_str::<serde_json::Map<String, Value>>(json_ld)
        .map_err(|e| { 
            eprintln!("Failed to parse JSON-LD: {}", e);
            Box::<dyn std::error::Error>::from(e)
        })?;
        scan_json_ld_obj(&mut loaded_json_ld, 6, false).await; // Call here for non-http case
        loaded_json_ld
    };
    
    info!(
        "{} Scanned Message: {:?}",
        DBP_RWD_BREWING_DEMAND, loaded_json_ld
    );

    let brewing_schema_name = loaded_json_ld[DBP_BREWER_INFO][SC_NAME].as_str().unwrap_or_else(||{
        eprintln!("Failed to extract brewing schema name");
        std::process::exit(1);
    });

    match brewing_schema_name {
        "dbpBrewerTemplate" => {
            info!("This is demand for DBP-BrewerTemplate program");

            // Extract brewing arguments
            let brewing_arguments= match utils::get_brewing_arguments(&loaded_json_ld){
                Ok(brewing_arguments) => brewing_arguments,
                Err(e) => {
                    eprintln!("Failed to extract brewing_arguments: {}", e);
                    return Err(e);
                }
            };
            info!("brewing_arguments: {:?}", brewing_arguments);

            // Extract output information
            let output_path = loaded_json_ld[DBP_BREWER_OUTPUT_STORE][DBP_BASE_URL]
                .as_str()
                .ok_or("Error: DBP_BASE_URL is missing")?;
            let data_output_path_pattern = loaded_json_ld[DBP_BREWER_OUTPUT_STORE][DBP_PATTERN]
                .as_str()
                .ok_or("Error: DBP_PATTERN is missing")?;
            let dt_start: DateTime<Local> = Into::into(
                DateTime::parse_from_rfc3339(
                    loaded_json_ld[DBP_TIME_PERIOD_START]
                    .as_str()
                    .ok_or("Error: DBP_TIME_PERIOD_START is missing")?
                ).map_err(|_| "Error: Invalid date format for DBP_TIME_PERIOD_START")?
            );
            let dt_end: DateTime<Local> = Into::into(
                DateTime::parse_from_rfc3339(
                    loaded_json_ld[DBP_TIME_PERIOD_END]
                    .as_str()
                    .ok_or("Error: DBP_TIME_PERIOD_END is missing")?
                ).map_err(|_| "Error: Invalid date format for DBP_TIME_PERIOD_END")?
            );
            let duration = utils::extract_minimum_unit(data_output_path_pattern)
                .ok_or("Error: Invalid output pattern")?;

            info!("output_path: {}", output_path);
            info!("data_output_path_pattern: {}", data_output_path_pattern);
            info!("dt_start: {}", dt_start);
            info!("dt_end: {}", dt_end);
            info!("duration: {:?}", duration);


            // Extract data_sets
            let data_sets = match extract_data_sets(&loaded_json_ld) {
                Ok(datasets) => datasets,
                Err(e) => {
                    eprintln!("Failed to extract datasets: {}", e);
                    return Err(e);
                }
            };
            info!("data_sets: {:?}", data_sets);
            
            for data_set in data_sets {
                if let Some(data_set_array) = data_set.as_array() {
                    for data_set_object in data_set_array {
                        if let Some(data_set_base_path) = data_set_object.get(DBP_BASE_URL).and_then(|v| v.as_str())
                        {
                            if let Some(pattern_value) =
                            data_set_object.get(DBP_PATTERN).and_then(|v| v.as_str())
                        {
                            match pattern_value {
                                pattern if pattern == data_output_path_pattern => {
                                    if pattern.ends_with(".extention") {
                                        info!("data_set_pattern: {}", pattern);
                                        match brewing_data_sample(pattern, data_set_base_path, &brewing_arguments, output_path, &dt_start, &dt_end, &duration) {
                                            Ok(_) => {
                                                info!("Sample data processed successfully for {}", data_set_base_path);
                                            }
                                            Err(e) => {
                                                error!("Error processing Sample data for {}: {}", data_set_base_path, e);
                                                return Err(e);
                                            }
                                        }
                                    } else {
                                        eprintln!("Error: Unknown data_set_pattern");
                                        return Err("Error: Unknown data_set_pattern".into());
                                    }
                                }
                                _ => {
                                    info!("data_set_pattern: {} does not match data_output_path_pattern: {}", pattern_value, data_output_path_pattern);
                                    return Err("Error: data_set_pattern does not match data_output_path_pattern".into()); 
                                }
                            }
                        } else {
                            eprintln!("Error: DBP_BASE_URL is missing");
                            return Err("Error: DBP_BASE_URL is missing".into());
                        }

                        } else {
                            eprintln!("Error: DBP_PATTERN is missing");
                            return Err("Error: DBP_PATTERN is missing".into());
                        }
                    }
                }
            }

            Ok(())
        }
        _ => {
            println!("This is NOT demand for this program");
            return Err("This is NOT demand for this program".into());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // get request
    let args = Args::parse();
    env::set_var("RUST_LOG", args.log_level);
    logger::init();

    let start_time = Local::now();
    info!("Started Program at {}", start_time.format("%F %T %:z"));

    println!("Received json_ld: {}", args.json_ld);
    process_demand(args.json_ld.as_str()).await?;
    let finish_time = Local::now();
    info!("Finished Program at {}", finish_time.format("%F %T %:z"));
    Ok(())
}