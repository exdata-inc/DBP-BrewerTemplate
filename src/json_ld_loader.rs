use async_recursion::async_recursion;
use serde_json::Value;

use dbp_schema::dbp_schema::{RealWorldDataset, RealWorldDataStructureInfo, RealWorldDataStructureItem, RealWorldDataStoringInfo, RealWorldDataCollectionInfo, EntryPoint};

fn string_getter(val: &Value, key: &str) -> Option<String> {
    if let Some(v) = val.get(key) { Some(String::from(v.as_str().unwrap())) } else { None }
}

fn i64_getter(val: &Value, key: &str) -> Option<i64> {
    if let Some(v) = val.get(key) { v.as_i64() } else { None }
}

fn time_getter(val: &Value, key: &str) -> Option<prost_types::Timestamp> {
    if let Some(v) = val.get(key) { 
        Some(prost_types::Timestamp { seconds: i64_getter(v, "seconds").unwrap_or(0), nanos: i64_getter(v, "nanos").unwrap_or(0) as i32 })
    } else { None }
}

fn id_getter(val: &Value) -> Option<String> {
    string_getter(val, "@id")
}

fn name_getter(val: &Value) -> Option<String> {
    string_getter(val, "schema:name")
}

fn url_getter(val: &Value) -> Option<String> {
    string_getter(val, "schema:url")
}

fn ref_getter(val: &Value) -> Option<String> {
    string_getter(val, "@ref")
}

async fn structure_info_getter(val: &Value) -> Option<RealWorldDataStructureInfo> {
    if let Some(si_json) = val.get("dbp:structureInfo") {
        if let Some(ref_url) = ref_getter(si_json) {
            info!("ref_url: {}", ref_url);
            let client = reqwest::Client::new();
            let resp = match client.get(ref_url).send().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            let body = match resp.text().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            match serde_json::from_str::<Value>(body.as_str()) {
                Ok(si_json) => Some(structure_info_parser(&si_json)),
                Err(_) => None,
            }
        } else {
            Some(structure_info_parser(si_json))
        }
    } else { None }
}

fn structure_info_parser(si_json: &Value) -> RealWorldDataStructureInfo {
    RealWorldDataStructureInfo {
        id: id_getter(si_json),
        name: name_getter(si_json),
        url: url_getter(si_json),
        encoding_format: string_getter(si_json, "schema:encodingFormat"),
        structure_items: if let Some(si_sis_json) = si_json.get("dbp:structureItems") {
            let mut si_sis = Vec::new();
            if let Some(si_sis_json) = si_sis_json.as_array() {
                for si_si_json in si_sis_json {
                    let si_si = RealWorldDataStructureItem {
                        id: id_getter(si_si_json),
                        name: name_getter(si_si_json),
                        url: url_getter(si_si_json),
                        structure_path: string_getter(si_si_json, "dbp:structurePath"),
                        item_type: string_getter(si_si_json, "dbp:itemType"),
                        item_vocab: string_getter(si_si_json, "dbp:itemVocab"),
                    };
                    si_sis.push(si_si);
                }
            }
            si_sis
        } else { Vec::new() },
    }
}


async fn distribution_getter(val: &Value) -> Option<RealWorldDataStoringInfo> {
    if let Some(d_json) = val.get("dbp:distribution") {
        if let Some(ref_url) = ref_getter(d_json) {
            info!("ref_url: {}", ref_url);
            let client = reqwest::Client::new();
            let resp = match client.get(ref_url).send().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            let body = match resp.text().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            match serde_json::from_str::<Value>(body.as_str()) {
                Ok(d_json) => Some(distribution_parser(&d_json)),
                Err(_) => None,
            }
        } else {
            Some(distribution_parser(d_json))
        }
    } else { None }
}

fn distribution_parser(d_json: &Value) -> RealWorldDataStoringInfo {
    RealWorldDataStoringInfo {
        id: id_getter(d_json),
        name: name_getter(d_json),
        url: url_getter(d_json),
        start_time: time_getter(d_json, "dbp:startTime"),
        end_time: time_getter(d_json, "dbp:endTime"),
        base_url: string_getter(d_json, "dbp:baseUrl"),
        pattern: string_getter(d_json, "dbp:pattern"),
    }
}


async fn collection_info_getter(val: &Value) -> Option<RealWorldDataCollectionInfo> {
    if let Some(ci_json) = val.get("dbp:collectionInfo") {
        if let Some(ref_url) = ref_getter(ci_json) {
            info!("ref_url: {}", ref_url);
            let client = reqwest::Client::new();
            let resp = match client.get(ref_url).send().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            let body = match resp.text().await {
                Ok(v) => v,
                Err(_) => { return None },
            };
            match serde_json::from_str::<Value>(body.as_str()) {
                Ok(ci_json) => Some(collection_info_parser(&ci_json)),
                Err(_) => None,
            }
        } else {
            Some(collection_info_parser(ci_json))
        }
    } else { None }
}

fn collection_info_parser(ci_json: &Value) -> RealWorldDataCollectionInfo {
    RealWorldDataCollectionInfo {
        id: id_getter(ci_json),
        name: name_getter(ci_json),
        url: url_getter(ci_json),
        collection_style: string_getter(ci_json, "dbp:collectionStyle"),
        collection_protocol: string_getter(ci_json, "dbp:collectionProtocol"),
        listen_address: string_getter(ci_json, "dbp:listenAddress"),
        server_address: string_getter(ci_json, "dbp:serverAddress"),
        entry_point: if let Some(ep) = ci_json.get("dbp:entryPoint") {
            Some(
                EntryPoint {
                    action_application: string_getter(ep, "schema:actionApplication"),
                    action_platform: string_getter(ep, "schema:actionPlatform"),
                    content_type: string_getter(ep, "schema:contentType"),
                    encoding_type: string_getter(ep, "schema:encodingType"),
                    http_method: string_getter(ep, "schema:httpMethod"),
                    url_template: string_getter(ep, "schema:urlTemplate"),
                    additional_type: string_getter(ep, "schema:additionalType"),
                    alternate_name: string_getter(ep, "schema:alternateName"),
                    description: string_getter(ep, "schema:description"),
                    disambiguating_description: string_getter(ep, "schema:disambiguatingDescription"),
                    id: id_getter(ep),
                    image: string_getter(ep, "schema:image"),
                    main_entity_of_page: string_getter(ep, "schema:mainEntityOfPage"),
                    name: name_getter(ep),
                    potential_action: string_getter(ep, "schema:potentialAction"),
                    same_as: string_getter(ep, "schema:sameAs"),
                    subject_of: string_getter(ep, "schema:subjectOf"),
                    url: url_getter(ep),
                }
            )
        } else { None },
    }
}

#[async_recursion]
async fn dataset_parser(rwd_json: &Value) -> RealWorldDataset {
    debug!("RWD: {:?}", rwd_json);
    RealWorldDataset {
        id: id_getter(rwd_json),
        name: name_getter(rwd_json),
        url: url_getter(rwd_json),
        structure_info: structure_info_getter(rwd_json).await,
        generated_from: if let Some(gfs_json) = rwd_json.get("dbp:generatedFrom") {
            let mut gf = Vec::new();
            if let Some(gfs_json) = gfs_json.as_array() {
                for gf_json in gfs_json {
                    gf.push(dataset_parser(gf_json).await);
                }
            }
            gf
        } else { Vec::new() },
        generated_using: None,
        generated_args: Vec::new(),
        collection_info: collection_info_getter(rwd_json).await,
        distribution: distribution_getter(rwd_json).await,
        author: string_getter(rwd_json, "schema:author"),
        content_location: string_getter(rwd_json, "schema:contentLocation"),
        date_created: time_getter(rwd_json, "schema:dateCreated"),
        date_modified: time_getter(rwd_json, "schema:dateModified"),
        date_published: time_getter(rwd_json, "schema:datePublished"),
        license: string_getter(rwd_json, "schema:license"),
        location_created: string_getter(rwd_json, "schema:locationCreated"),
        description: string_getter(rwd_json, "schema:description"),
    }
}

pub async fn get_real_world_datasets(
    url: &str,
) -> Result<Vec<RealWorldDataset>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await?;
    let body = resp.text().await?;

    let mut rwds: Vec<RealWorldDataset> = Vec::new();

    if let Some(rwds_json) = serde_json::from_str::<Value>(body.as_str()).unwrap().as_array() {
        for rwd_json in rwds_json {
            debug!("RWD: {:?}", rwd_json);
            let rwd = dataset_parser(rwd_json).await;
            rwds.push(rwd);
        }
    }


    Ok(rwds)
}
