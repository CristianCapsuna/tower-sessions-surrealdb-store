use super::*;
use std::collections::HashMap;
use serde_json::{
    json
    , value::Value
};
use tower_sessions::cookie::time::{
    OffsetDateTime
    , Duration
};
use anyhow::anyhow;

#[tokio::test]
async fn record_lifecycle() -> anyhow::Result<()> {
    let store = SurrealdbStore::new_from_nothing(
            "ws".into()
            , "localhost:8000".into()
            , "root".into()
            , "namespace".into()
            , "database".into()
            , "sessions".into()
            , "sessions_latest_id".into()
        ).await.unwrap();
    store.create_data_model().await?;
    let mut test_hash: HashMap<String, Value> = HashMap::new();
    test_hash.insert(
        "test_key_1".into()
        , json!("test_value_1")
    );
    let mut my_record = Record {
        id: Id(5)
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

    store.delete(&my_record.id).await
        .context(format!("Could not delete errorwith id: {}", &my_record.id.clone()))?;
    let result = store.load(&my_record.id).await
        .context(format!("Could not load record after delete with id: {}", &my_record.id.clone()))?;
    assert!(result.is_none());
    Ok(())
}