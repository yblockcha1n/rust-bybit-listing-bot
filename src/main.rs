use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH, Duration as StdDuration};
use chrono::{Duration, Local, NaiveDateTime, TimeZone};

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;

use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct Config {
    api_key: String,
    secret_key: String,
    recv_window: String,
    url: String,
    symbol: String,
    usdt_quantity: String,
    order_time: String,
    order_interval: u64,
    limit_price_multiplier: f64,
    coin: String,
}

#[derive(Debug, Deserialize)]
struct BalanceResponse {
    retCode: i32,
    retMsg: String,
    result: BalanceResult,
}

#[derive(Debug, Deserialize)]
struct BalanceResult {
    list: Vec<AccountInfo>,
}

#[derive(Debug, Deserialize)]
struct AccountInfo {
    coin: Vec<CoinInfo>,
}

#[derive(Debug, Deserialize)]
struct CoinInfo {
    coin: String,
    walletBalance: String,
}

#[tokio::main]
async fn main() {
    let mut config_file = File::open("src/config.json").unwrap();
    let mut config_str = String::new();
    config_file.read_to_string(&mut config_str).unwrap();
    let config: Config = serde_json::from_str(&config_str).unwrap();

    let api_key = config.api_key;
    let secret_key = config.secret_key;
    let recv_window = config.recv_window;
    let url = config.url;
    let symbol = config.symbol;
    let usdt_quantity = config.usdt_quantity;
    let order_time = config.order_time;
    let order_interval = config.order_interval;

    let order_time_dt = Local.from_local_datetime(&NaiveDateTime::parse_from_str(&format!("{} {}", Local::now().format("%Y-%m-%d"), order_time), "%Y-%m-%d %H:%M:%S").unwrap()).unwrap();
    let order_start_time = order_time_dt - Duration::seconds(order_interval as i64);

    let order_placed = Arc::new(Mutex::new(false));
    let mut handles = vec![];
    let token_quantity_mutex = Arc::new(Mutex::new(String::new()));
    let actual_buy_price_mutex = Arc::new(Mutex::new(0.0));

    let initial_balance = 0.0; // 初期残高を0に設定

    while !*order_placed.lock().await {
        let now = Local::now();
        if now >= order_start_time && now < order_time_dt {
            let mut price_params = HashMap::new();
            price_params.insert("category", "spot");
            price_params.insert("symbol", &symbol);

            loop {
                let price_response: serde_json::Value = http_get_request_with_params("/v5/market/tickers", &price_params, &api_key, &secret_key, &recv_window, &url).await;

                if price_response["retCode"] == 0 {
                    if let Some(last_price_str) = price_response["result"]["list"][0]["lastPrice"].as_str() {
                        if let Ok(last_price) = last_price_str.parse::<f64>() {
                            let truncated_price = truncate_price(last_price);
                            let limit_price = truncate_price(truncated_price * 1.005); // 指値価格を0.5%上げてtruncate_price関数で調整
                            let adjusted_usdt_quantity = usdt_quantity.parse::<f64>().unwrap() * 0.999; // 取引手数料0.1%を差し引く
                            let token_quantity = calculate_token_quantity(adjusted_usdt_quantity, limit_price); // 0.1%上げた指値価格をベースに計算
                            *token_quantity_mutex.lock().await = token_quantity.clone();

                            // 購入注文の処理
                            for _ in 0..5 {
                                for _ in 0..20 {
                                    let api_key = api_key.clone();
                                    let secret_key = secret_key.clone();
                                    let recv_window = recv_window.clone();
                                    let url = url.clone();
                                    let symbol = symbol.clone();
                                    let order_placed = Arc::clone(&order_placed);
                                    let token_quantity = token_quantity.clone();

                                    let handle = tokio::spawn(async move {
                                        let endpoint = "/v5/order/create";
                                        let method = Method::POST;
                                        let order_link_id = Uuid::new_v4().to_string();
                                        let params = json!({
                                            "category": "spot",
                                            "symbol": symbol,
                                            "side": "Buy",
                                            "orderType": "Limit",
                                            "qty": token_quantity,
                                            "price": limit_price.to_string(),
                                            "orderLinkId": order_link_id,
                                        });

                                        let order_response = http_request(endpoint, method, params, "Create", &api_key, &secret_key, &recv_window, &url).await;

                                        if order_response.contains("\"retCode\":0") {
                                            let mut order_placed_lock = order_placed.lock().await;
                                            *order_placed_lock = true;
                                        }
                                    });

                                    handles.push(handle);
                                }

                                while let Some(handle) = handles.pop() {
                                    handle.await.unwrap();
                                }

                                if *order_placed.lock().await {
                                    break;
                                }

                                sleep(StdDuration::from_millis(1000)).await;
                            }

                            if !*order_placed.lock().await {
                                continue;
                            }
                            break;
                        }
                    }
                }
                println!("Error getting price data. Retrying...");
                sleep(StdDuration::from_millis(1000)).await;
            }
        }
        sleep(StdDuration::from_millis(66)).await;
    }

    let _token_quantity = token_quantity_mutex.lock().await.clone();

    let mut balance_increased = false;
    let mut price_params = HashMap::new();
    price_params.insert("category", "spot");
    price_params.insert("symbol", &symbol);
    
    while !balance_increased {
        let (current_balance, current_price) = tokio::join!(
            async {
                loop {
                    match get_wallet_balance(&api_key, &secret_key, &recv_window, &url, &config.coin).await {
                        Ok(balance) => break balance,
                        Err(e) => {
                            println!("Error getting wallet balance: {:?}. Retrying...", e);
                            sleep(StdDuration::from_millis(1000)).await;
                        }
                    }
                }
            },
            async {
                loop {
                    match get_current_price(&price_params, &api_key, &secret_key, &recv_window, &url).await {
                        Ok(price) => {
                            println!("Current price: {}", price);
                            sleep(StdDuration::from_millis(1000)).await;
                            break price;
                        }
                        Err(e) => {
                            println!("Error getting current price: {:?}. Retrying...", e);
                            sleep(StdDuration::from_millis(1000)).await;
                        }
                    }
                }
            }
        );
    
        if current_balance > initial_balance {
            balance_increased = true;
            *actual_buy_price_mutex.lock().await = current_price;
        }
    
        sleep(StdDuration::from_millis(1000)).await;
    }

    let actual_buy_price = *actual_buy_price_mutex.lock().await;
    let limit_price = actual_buy_price * config.limit_price_multiplier;

    let mut price_params = HashMap::new();
    price_params.insert("category", "spot");
    price_params.insert("symbol", &symbol);

    let price_response: serde_json::Value = http_get_request_with_params("/v5/market/tickers", &price_params, &api_key, &secret_key, &recv_window, &url).await;
    if price_response["retCode"] == 0 {
        let mut last_price = price_response["result"]["list"][0]["lastPrice"].as_str().unwrap().parse::<f64>().unwrap();
        let mut truncated_price = truncate_price(last_price);
        println!("Current price: {}", truncated_price);

        while truncated_price < limit_price {
            for _ in 0..10 {
                let price_response: serde_json::Value = http_get_request_with_params("/v5/market/tickers", &price_params, &api_key, &secret_key, &recv_window, &url).await;
                if price_response["retCode"] == 0 {
                    last_price = price_response["result"]["list"][0]["lastPrice"].as_str().unwrap().parse::<f64>().unwrap();
                    truncated_price = truncate_price(last_price);
                    println!("Current price: {}", truncated_price);

                    if truncated_price >= limit_price {
                        break;
                    }
                }

                sleep(StdDuration::from_millis(100)).await;
            }

            sleep(StdDuration::from_millis(1000)).await;
        }

        // 売却処理
        let sell_order_placed = Arc::new(Mutex::new(false));
        while !*sell_order_placed.lock().await {
            let current_balance = match get_wallet_balance(&api_key, &secret_key, &recv_window, &url, &config.coin).await {
                Ok(balance) => balance,
                Err(_) => continue,
            };

            let sell_endpoint = "/v5/order/create";
            let sell_method = Method::POST;
            let sell_order_link_id = Uuid::new_v4().to_string();
            let sell_limit_price = truncate_price(truncated_price * 0.997); // 指値価格を0.3%下げてtruncate_price関数で調整

            let sell_qty: String;
            if sell_limit_price < 0.1 {
                sell_qty = format!("{:.0}", current_balance * 0.999); // 整数のみ
            } else if sell_limit_price < 10.0 {
                sell_qty = format!("{:.1}", current_balance * 0.999); // 小数第1以下切り捨て(一旦、2ドル未満)
            } else if sell_limit_price < 100.0 {
                sell_qty = format!("{:.2}", current_balance * 0.999); // 小数第2以下切り捨て 
            } else if sell_limit_price < 10000.0 {
                sell_qty = format!("{:.4}", current_balance * 0.999); // 小数第4以下切り捨て
            } else {
                sell_qty = format!("{:.5}", current_balance * 0.999); // 小数第5以下切り捨て
            }

            let sell_params = json!({
                "category": "spot",
                "symbol": symbol,
                "side": "Sell",
                "orderType": "Limit",
                "qty": sell_qty,
                "price": sell_limit_price.to_string(),
                "orderLinkId": sell_order_link_id,
            });

            let sell_order_response = http_request(sell_endpoint, sell_method, sell_params, "Sell", &api_key, &secret_key, &recv_window, &url).await;
            println!("Sell Order Response: {:?}", sell_order_response);

            if sell_order_response.contains("\"retCode\":0") {
                let mut sell_order_placed_lock = sell_order_placed.lock().await;
                *sell_order_placed_lock = true;
            }

            sleep(StdDuration::from_millis(100)).await;
        }
    }
}

async fn get_wallet_balance(api_key: &str, secret_key: &str, recv_window: &str, url: &str, coin: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let balance_endpoint = "/v5/account/wallet-balance";
    let mut params = HashMap::new();
    params.insert("accountType", "UNIFIED");
    params.insert("coin", coin);
    let balance_response: serde_json::Value = http_get_request_with_params(balance_endpoint, &params, api_key, secret_key, recv_window, url).await;

    if balance_response["retCode"] == 0 {
        if let Some(account_info) = balance_response["result"]["list"].as_array() {
            if let Some(coin_info) = account_info[0]["coin"].as_array() {
                if let Some(wallet_balance) = coin_info[0]["walletBalance"].as_str() {
                    let token_balance = wallet_balance.parse::<f64>()?;
                    println!("Token Balance: {}", token_balance);
                    return Ok(token_balance);
                }
            }
        }
    }

    Err("Failed to get wallet balance".into())
}

async fn get_current_price(price_params: &HashMap<&str, &str>, api_key: &str, secret_key: &str, recv_window: &str, url: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let price_response: serde_json::Value = http_get_request_with_params("/v5/market/tickers", price_params, api_key, secret_key, recv_window, url).await;
    if price_response["retCode"] == 0 {
        let last_price = price_response["result"]["list"][0]["lastPrice"].as_str().unwrap().parse::<f64>()?;
        let truncated_price = truncate_price(last_price);
        return Ok(truncated_price);
    }

    Err("Failed to get current price".into())
}

fn truncate_price(price: f64) -> f64 {
    if price < 0.0001 {
        (price * 100000.0).trunc() / 100000.0
    } else if price < 0.001 {
        (price * 10000.0).trunc() / 10000.0
    } else if price < 0.01 {
        (price * 1000.0).trunc() / 1000.0
    } else if price < 0.1 {
        (price * 100.0).trunc() / 100.0
    } else if price < 1.0 {
        (price * 10.0).trunc() / 10.0
    } else if price < 20.0 {
        (price * 10000.0).trunc() / 10000.0
    } else {
        price.trunc()
    }
}
fn calculate_token_quantity(usdt_quantity: f64, price: f64) -> String {
    let token_quantity = usdt_quantity / price;
    if price < 1.0 {
        format!("{:.0}", token_quantity)
    } else if price < 100.0 {
        format!("{:.1}", token_quantity)
    } else if price < 1000.0 {
        format!("{:.3}", token_quantity)
    } else if price < 10000.0 {
        format!("{:.4}", token_quantity)
    } else {
        format!("{:.5}", token_quantity)
    }
 }
 
 async fn http_get_request_with_params(endpoint: &str, params: &HashMap<&str, &str>, api_key: &str, secret_key: &str, recv_window: &str, url: &str) -> serde_json::Value {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
 
    let signature = gen_signature(&timestamp, &generate_query_str(params), secret_key, api_key, recv_window);
 
    let mut headers = HeaderMap::new();
    headers.insert("X-BAPI-API-KEY", HeaderValue::from_str(api_key).unwrap());
    headers.insert("X-BAPI-SIGN", HeaderValue::from_str(&signature).unwrap());
    headers.insert("X-BAPI-SIGN-TYPE", HeaderValue::from_static("2"));
    headers.insert("X-BAPI-TIMESTAMP", HeaderValue::from_str(&timestamp).unwrap());
    headers.insert("X-BAPI-RECV-WINDOW", HeaderValue::from_str(recv_window).unwrap());
 
    let client = Client::new();
    let response = client
        .get(&format!("{}{}", url, endpoint))
        .query(params)
        .headers(headers)
        .send()
        .await;
 
    match response {
        Ok(resp) => resp.json().await.unwrap_or_else(|_| serde_json::json!(null)),
        Err(_) => serde_json::json!(null),
    }
 }
 
 async fn http_request(endpoint: &str, method: Method, payload: serde_json::Value, info: &str, api_key: &str, secret_key: &str, recv_window: &str, url: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
 
    let signature = gen_signature(&timestamp, &payload.to_string(), secret_key, api_key, recv_window);
 
    let mut headers = HeaderMap::new();
    headers.insert("X-BAPI-API-KEY", HeaderValue::from_str(api_key).unwrap());
    headers.insert("X-BAPI-SIGN", HeaderValue::from_str(&signature).unwrap());
    headers.insert("X-BAPI-SIGN-TYPE", HeaderValue::from_static("2"));
    headers.insert("X-BAPI-TIMESTAMP", HeaderValue::from_str(&timestamp).unwrap());
    headers.insert("X-BAPI-RECV-WINDOW", HeaderValue::from_str(recv_window).unwrap());
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
 
    let client = Client::new();
    let start_time = std::time::Instant::now();
    let response = match method {
        Method::POST => client
            .post(&format!("{}{}", url, endpoint))
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .unwrap(),
        _ => client
            .request(method, &format!("{}{}?{}", url, endpoint, payload))
            .headers(headers)
            .send()
            .await
            .unwrap(),
    };
    let elapsed_time = start_time.elapsed();
 
    let response_text = response.text().await.unwrap();
    println!("{} Response: {}", info, response_text);
    println!("{} Elapsed Time: {:?}", info, elapsed_time);
 
    response_text
 }
 
 fn generate_query_str(params: &HashMap<&str, &str>) -> String {
    let mut query_str = String::new();
    for (key, value) in params {
        if !query_str.is_empty() {
            query_str.push('&');
        }
        query_str.push_str(&format!("{}={}", key, value));
    }
    query_str
 }
 
 fn gen_signature(timestamp: &str, payload: &str, secret_key: &str, api_key: &str, recv_window: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes()).unwrap();
    let param_str = format!("{}{}{}{}", timestamp, api_key, recv_window, payload);
    mac.update(param_str.as_bytes());
    hex::encode(mac.finalize().into_bytes())
 }