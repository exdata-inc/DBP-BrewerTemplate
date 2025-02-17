// Standard Library
use std::fs;
use std::path::Path;

// External Library
use chrono::{DateTime, Duration, Local};
use json_ld_utils::DBP_BREWER_INPUT;
use json_ld_utils::SC_DATASET;
use json_ld_utils::SC_DISTRIBUTION;
use serde_json::{Map, Value};
use regex::Regex;
use json_ld_utils::{DBP_BREWING_ARGUMENT, DBP_KEY, SC_VALUE};

use crate::protocols;

pub fn mkdir_to_dest(
    url: &str,
    dt_start: DateTime<Local>,
    dt_end: DateTime<Local>,
    step: Duration,
) {
    match url {
        _ if url.starts_with(protocols::FILE) => {
            // Local File System
            let fs_template_path = url.replace(protocols::FILE, "");
            let fs_dir_path = Path::new(fs_template_path.as_str())
                .parent()
                .unwrap()
                .to_str()
                .unwrap();
            let mut dt = dt_start;
            while dt <= dt_end {
                // Create Destination Directory
                match fs::create_dir_all(dt.format(fs_dir_path).to_string().as_str()) {
                    Ok(_) => debug!(
                        "Directories created successfully (or already exists): {}",
                        dt.format(fs_dir_path).to_string().as_str()
                    ),
                    Err(e) => error!("Error creating directories: {}", e),
                };
                dt += step;
            }
        }
        _ if url.starts_with(protocols::FTP) => {} // TODO: Implement this!
        _ if url.starts_with(protocols::HTTP) => {} // TODO: Implement this!
        _ if url.starts_with(protocols::HTTPS) => {} // TODO: Implement this!
        _ => error!("Unknown Protocol!"),
    }
}

pub fn extract_minimum_unit(output_pattern: &str) -> Option<Duration> {
    let re = Regex::new(r"%([YmdHMS])").unwrap_or_else(|e| {
        eprintln!("Failed to compile regex: {}", e);
        std::process::exit(1);
    });
    let captures: Vec<_> = re.captures_iter(output_pattern).collect();

    let mut min_duration: Option<Duration> = None;

    for capture in captures {
        let duration = match &capture[1] {
            "Y" => Some(Duration::days(365)),
            "m" => Some(Duration::days(30)),
            "d" => Some(Duration::days(1)),
            "H" => Some(Duration::hours(1)),
            "M" => Some(Duration::minutes(1)),
            "S" => Some(Duration::seconds(1)),
            _ => None,
        };

        if let Some(d) = duration {
            min_duration = match min_duration {
                Some(existing) => Some(if d < existing { d } else { existing }),
                None => Some(d),
            };
        }
    }

    min_duration
}


pub fn extract_data_sets(
    loaded_json_ld: &Map<String, Value>,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let base_array = loaded_json_ld
        .get(DBP_BREWER_INPUT)
        .and_then(|v| v.as_array())
        .ok_or("Failed to get DBP_BREWER_INPUT array")?;

    let mut data_sets = Vec::new();

    for element in base_array {
        if let Some(dataset_object) = element.get(SC_DATASET).and_then(|v| v.as_object()) {
            if let Some(distribution_array) = dataset_object.get(SC_DISTRIBUTION) {
                data_sets.push(distribution_array.clone());
            }
        }
    }

    Ok(data_sets)
}

pub fn get_brewing_arguments(v: &Map<String, Value> )-> Result<Vec<Map<String, Value>>, Box<dyn std::error::Error>> {
    let mut brewing_arguments_array: Vec<Map<String, Value>> = Vec::new(); 
    if let Some(brewing_argument) = v[DBP_BREWING_ARGUMENT].as_array() {
        for arg in brewing_argument {
            if let Some(key) = arg[DBP_KEY].as_str() {
                if key == "sample_key" {
                    if let Some(value) = arg[SC_VALUE].as_str() {
                        let mut map: Map<String, Value> = Map::new();
                        map.insert(key.to_string(), Value::String(value.to_string())); 
                        brewing_arguments_array.push(map);
                    }
                }
            }
        }
        return Ok(brewing_arguments_array);
    }
    else{
        eprintln!("Error: DBP_BREWING_ARGUMENT is missing");
        return Err("Error: DBP_BREWING_ARGUMENT is missing".into());
    }
}