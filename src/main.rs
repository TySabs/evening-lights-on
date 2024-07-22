use chrono::{DateTime, Duration, Local, Utc};
use dotenv::dotenv;
use reqwest::Error as ReqwestError;
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio_postgres::Client;
use tokio_postgres::NoTls;

#[derive(Deserialize)]
struct SunriseSunsetResponse {
    results: SunriseSunsetResults,
}

#[derive(Deserialize)]
struct SunriseSunsetResults {
    sunset: String,
}

struct WizLight {
    host_id: String,
    name: String,
}

#[derive(Error, Debug)]
enum SunsetError {
    #[error("HTTP request error")]
    ReqwestError(#[from] ReqwestError),
    #[error("DateTime parse error")]
    ChronoParseError(#[from] chrono::ParseError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let db_host = env::var("DB_HOST").expect("DB_HOST not set");
    let db_user = env::var("DB_USER").expect("DB_USER not set");
    let db_password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
    let db_name: String = env::var("DB_NAME").expect("DB_NAME not set");

    let conn_str = format!(
        "host={} user={} password={} dbname={}",
        db_host, db_user, db_password, db_name
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // Spawn the connection to run in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let wiz_lights = fetch_wiz_lights(&client).await?;

    let sunset_utc = fetch_sunset_time().await?;
    let sunset_local = sunset_utc.with_timezone(&Local);
    let turn_on_time = sunset_local - Duration::minutes(45);

    let now = Local::now();
    let duration_to_wait = turn_on_time - now;

    if duration_to_wait.num_seconds() > 0 {
        let message = format!(
            "Sunset local time is {}. Sleeping for {} seconds until {} before turning lights on.",
            sunset_local,
            duration_to_wait.num_seconds(),
            turn_on_time
        );
        println!("{}", message);
        log_light_event(&client, "Info", &message, "All").await?;
        tokio::time::sleep(duration_to_wait.to_std()?).await;
    } else {
        let message = format!("It is already close enough to sunset. Sunset local time is {}. Turning lights on immediately.", sunset_local);
        println!("{}", message);
        log_light_event(&client, "Info", &message, "All").await?;
    }

    // Turn the lights on
    let payload_on = r#"{"method":"setPilot","params":{"state":true,"dimming":100}}"#;
    for light in &wiz_lights {
        match send_udp_packet(&light.host_id, payload_on).await {
            Ok(_) => {
                let severity: &str = "Info";
                let message = format!("Light {} at {} turned on!", light.name, light.host_id);
                println!("SUCCESS: {}", message);
                log_light_event(&client, severity, &message, &light.name).await?;
            }
            Err(e) => {
                let severity: &str = "Error";
                let message = format!(
                    "Failed to turn on light {} at {}: {}",
                    light.name, light.host_id, e
                );
                println!("ERROR: {}", message);
                log_light_event(&client, severity, &message, &light.name).await?;
            }
        }
    }

    Ok(())
}

/// Gets wiz lights from postgres db
async fn fetch_wiz_lights(client: &Client) -> Result<Vec<WizLight>, Box<dyn std::error::Error>> {
    let rows = client
        .query("SELECT host_id, name FROM machine", &[])
        .await?;

    let network_id: String = env::var("NETWORK_ID").expect("NETWORK_ID not set");

    let mut wiz_lights = Vec::new();
    for row in rows {
        let host_id: String = row.get("host_id");
        let name: String = row.get("name");
        wiz_lights.push(WizLight {
            host_id: format!("{}.{}:38899", network_id, host_id),
            name,
        });
    }

    Ok(wiz_lights)
}

/// Gets sunset time from sunrise/sunset api
async fn fetch_sunset_time() -> Result<DateTime<Utc>, SunsetError> {
    let lat: f64 = env::var("LAT")
        .expect("LAT not set")
        .parse()
        .expect("Invalid latitude value");
    let lng: f64 = env::var("LNG")
        .expect("LNG not set")
        .parse()
        .expect("Invalid longitude value");

    let url = format!(
        "https://api.sunrise-sunset.org/json?lat={}&lng={}&formatted=0",
        lat, lng
    );

    let resp = reqwest::get(&url)
        .await?
        .json::<SunriseSunsetResponse>()
        .await?;
    let sunrise_utc = resp.results.sunset.parse::<DateTime<Utc>>()?;
    Ok(sunrise_utc)
}

/// Sends a given payload to a given ip address
async fn send_udp_packet(addr: &str, payload: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let addr: SocketAddr = addr.parse()?;
    socket.send_to(payload.as_bytes(), &addr).await?;
    Ok(())
}

/// Logs a light event in the postgres db
async fn log_light_event(
    client: &Client,
    severity: &str,
    message: &str,
    machine: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_type = "Evening";

    client
        .execute(
            "INSERT INTO log (severity, message, machine, event_type) VALUES ($1, $2, $3, $4)",
            &[&severity, &message, &machine, &event_type],
        )
        .await?;

    Ok(())
}
