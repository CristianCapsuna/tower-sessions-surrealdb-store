use super::*;
use std::{
    collections::HashMap
    , env::current_dir
};
use serde_json::{
    json
    , value::Value
};
use tower_sessions::cookie::time::{
    OffsetDateTime
    , Duration
};
use anyhow::{anyhow, Context};
use std::sync::LazyLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing::debug;

static LOGGING_INIT: LazyLock<WorkerGuard> = LazyLock::new(|| {
    let file_appender = tracing_appender::rolling::hourly(current_dir().unwrap(), "log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .init();
    debug!("Logger initialised");
    guard
});

async fn create_store() -> anyhow::Result<SurrealdbStore<Any>> {
    Ok(SurrealdbStore::new_from_nothing(
        "ws".into()
        , "localhost:8000".into()
        , "root".into()
        , "namespace".into()
        , "database".into()
        , "sessions".into()
        , "sessions_latest_id".into()
    ).await.context("Connecting to SurrealDB with the specified config failed")?)
}

#[tokio::test]
async fn record_lifecycle() -> anyhow::Result<()> {
    let _ = *LOGGING_INIT;
    let store = create_store().await?;
    store.create_data_model().await?;
    let mut test_hash: HashMap<String, Value> = HashMap::new();
    test_hash.insert(
        "test_key_1".into()
        , json!("test_value_1")
    );
    let mut my_record = Record {
        id: Id(0)
        , data: test_hash
        , expiry_date: OffsetDateTime::now_utc().saturating_add(Duration::weeks(1))
    };

    // test create and load
    store.create(&mut my_record).await
        .context(format!("Could not create record. Record was: {:#?}"
            , my_record))?;
    let result = store.load(&my_record.id).await
        .context(format!("Could not load record after create with id: {}", &my_record.id.clone()))?;
    let loaded_after_create = result.ok_or(anyhow!("Load after create was successfull but no data was returned"))?;
    assert_eq!(my_record, loaded_after_create);
    
    // test update

    my_record.data.insert("test_key_2".into(), json!("test_value_2"));
    store.save(&my_record).await
        .context(format!("Could not save record. Record was: {:#?}", my_record))?;
    let result = store.load(&my_record.id).await
        .context(format!("Could not load record after save with id: {}", &my_record.id.clone()))?;
    let loaded_after_save = result.ok_or(anyhow!("Load after save was successfull but no data was returned"))?;
    assert_eq!(my_record, loaded_after_save);

    // test delete

    store.delete(&my_record.id).await
        .context(format!("Could not delete errorwith id: {}", &my_record.id.clone()))?;
    let result = store.load(&my_record.id).await
        .context(format!("Could not load record after delete with id: {}", &my_record.id.clone()))?;
    assert!(result.is_none());
    Ok(())
}

#[tokio::test]
async fn removal_of_expired() -> anyhow::Result<()> {
    let _ = *LOGGING_INIT;
    let store = create_store().await?;
    store.create_data_model().await?;
    let mut test_hash: HashMap<String, Value> = HashMap::new();
    test_hash.insert(
        "test_key_1".into()
        , json!("test_value_1")
    );
    let mut past_record = Record {
        id: Id(999)
        , data: test_hash.clone()
        , expiry_date: OffsetDateTime::now_utc().saturating_sub(Duration::minutes(5))
    };
    store.create(&mut past_record).await
        .context(format!("Could not create past record. Record was: {:#?}"
            , past_record))?;
    store.delete_expired().await.context("Deletion on past record failed")?;
    let result = store.load(&past_record.id).await
        .context(format!("Could not load past record with id: {}", &past_record.id.clone()))?;
    // assert!(result.is_none());
    if let Some(record) = result {
        return Err(anyhow!("Instead of none, record was returned. Record was: {:#?}", record))
    };

    let mut future_record = Record {
        id: Id(999)
        , data: test_hash.clone()
        , expiry_date: OffsetDateTime::now_utc().saturating_add(Duration::minutes(5))
    };
    store.create(&mut future_record).await
        .context(format!("Could not create future record. Record was: {:#?}"
            , future_record))?;
    store.delete_expired().await.context("Deletion on past record failed")?;
    let result = store.load(&future_record.id).await
        .context(format!("Could not load past record with id: {}", &future_record.id.clone()))?;
    let loaded_future_record = result.ok_or(anyhow!("Load of future record was successfull but no data was returned"))?;
    assert_eq!(future_record, loaded_future_record);
    Ok(())
}