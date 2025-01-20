use surrealdb::{
    Surreal
    , Connection
    , Datetime
    , engine::any::Any
    , opt::auth::Root
};
use tower_sessions::{
    ExpiredDeletion
    , SessionStore
    , session::{Id, Record}
    , session_store
    };
use tower_sessions::cookie::time::{
    OffsetDateTime
    , format_description::well_known::{
        Rfc3339
        , Iso8601
        , iso8601::{
            TimePrecision
            , Config
            , EncodedConfig
        }
    }
    , Duration
};
use chrono;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap
    , env::var
    , error::Error
    , num::NonZeroU8
    , fmt::Debug
};
use serde_json::{Value, json};
use async_trait::async_trait;
use base64::{
    prelude::BASE64_STANDARD_NO_PAD
    , Engine
};

const FORMAT_CONFIG: EncodedConfig = Config::DEFAULT.set_time_precision(
    TimePrecision::Second{decimal_digits: NonZeroU8::new(6)}
).encode();

fn surreal_to_ts_error_conv(surreal_error: surrealdb::Error) -> tower_sessions::session_store::Error {
    tower_sessions::session_store::Error::Backend(surreal_error.to_string())
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "Id")]
enum SurrealId {
    Number(i64)
}

#[derive(Serialize, Deserialize)]
struct RecordId {
    #[serde(rename = "tb")]
    table_name: String
    , id: SurrealId
}

#[derive(Serialize, Deserialize, Debug)]
struct DatabaseRecord {
    #[serde(with = "serde_bytes")]
    record: Vec<u8>
    , expiry_date: Datetime
}

impl From<&Record> for DatabaseRecord {
    fn from(record: &Record) -> Self {
        let interim_datetime_string = record.expiry_date.format(&Rfc3339)
            .expect("interim_datetime_string conversion from record expiry date failed");
        let chrono_datetime = interim_datetime_string.parse::<chrono::DateTime<chrono::offset::Utc>>()
            .expect("chrono_datetime creation failed");

        Self {
            record: rmp_serde::to_vec(record)
                .expect("MessagePack encoding should never fail because the library creator uses rmp_serde himself")
            , expiry_date: Datetime::from(chrono_datetime)
        }
    }
}

impl From<DatabaseRecord> for Record {
    fn from(database_record: DatabaseRecord) -> Record {
        rmp_serde::from_slice(&database_record.record).expect("Shouldn't fail here because the record was used to put the data here")
    }
}

#[derive(Clone, Debug)]
pub struct SurrealdbStore<DB>
where
    DB: Connection + Debug
{
    client: Surreal<DB>,
    sessions_table: String,
    sessions_latest_id_table: String
}

impl<DB> SurrealdbStore<DB>
where
    DB: Connection + Debug
{
    pub async fn new(
        client: Surreal<DB>
        , sessions_table: String
        , sessions_latest_id_table: String
    ) -> Self
    {
        Self {
            client: client
            , sessions_table: sessions_table
            , sessions_latest_id_table: sessions_latest_id_table
        }
    }
    
    pub async fn create_data_model(&self) -> Result<(), Box<dyn Error>> {
        self.client.query("DEFINE TABLE IF NOT EXISTS $sessions_table SCHEMAFULL;")
            .bind(("sessions_table", self.sessions_table.clone()))
            .query("DEFINE FIELD IF NOT EXISTS id ON TABLE $sessions_table TYPE int;")
            .bind(("sessions_table", self.sessions_table.clone()))
            .query("DEFINE FIELD IF NOT EXISTS expiry_date ON TABLE $sessions_table TYPE datetime;")
            .bind(("sessions_table", self.sessions_table.clone()))
            .query("DEFINE FIELD IF NOT EXISTS record ON TABLE $sessions_table TYPE array<int>;")
            .bind(("sessions_table", self.sessions_table.clone()))
            .await?;
        Ok(())
    }
}

impl SurrealdbStore<Any> {
    pub async fn new_from_env() -> Result<Self, Box<dyn Error>> {
        // Connect to the database
        let db_endpoint = match var("DB_ENDPOINT") {
            Ok(val) => val
            , Err(_e) => "memory".to_owned()
        };
        let db_username = match var("DB_USERNAME") {
            Ok(val) => val
            , Err(_e) => {
                println!("DB_USERNAME env var not defined");
                panic!()
            }
        };
        let db_password = match var("DB_PASSWORD") {
            Ok(val) => val
            , Err(_e) => {
                println!("DB_PASSWORD env var not defined");
                panic!()
            }
        };

        let surreal_connection: Surreal<Any> = Surreal::init();
        surreal_connection.connect(format!("ws://{db_endpoint}")).await?;
        
        // Log into the database
        surreal_connection.signin(Root {
            username: db_username.as_str(),
            password: db_password.as_str(),
        }).await?;
        
        // Select a namespace/database
        surreal_connection.use_ns("namespace").use_db("database").await?;
        Ok(
            Self {
                client: surreal_connection
                , sessions_table: "sessions".into()
                , sessions_latest_id_table: "sessions_latest_id".into()
            }
        )
    }
}

#[async_trait]
impl<DB> ExpiredDeletion for SurrealdbStore<DB>
where
    DB: Connection + Debug
{
    async fn delete_expired(&self) -> session_store::Result<()> {
        self.client.query(
                r#"delete $table
                where expiry_date <= time::unix(time::now())"#
            ).bind(("table", self.sessions_table.clone()))
            .await
            .map_err(surreal_to_ts_error_conv)?
            .check()
            .map_err(surreal_to_ts_error_conv)?;
        Ok(())
    }
}

#[async_trait]
impl<DB> SessionStore for SurrealdbStore<DB>
where
    DB: Connection + Debug
{
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let record_reference = &*record;
        let surrealdb_record: DatabaseRecord = record_reference.into();
        let datetime_string = record_reference.expiry_date
            .format(&Iso8601::<{FORMAT_CONFIG}>)
            .expect("datetime_string creation from Record expiry_date failed");
        // let record_data = BASE64_STANDARD_NO_PAD.encode(surrealdb_record.record.to_vec());
        let record_data = BASE64_STANDARD_NO_PAD.encode(surrealdb_record.record);
        let result: Option<RecordId> = self.client.query(r#"
                BEGIN TRANSACTION;
                UPSERT type::thing($sessions_latest_id_table, "counter") SET num += 1;
                CREATE type::thing($sessions_table, type::thing($sessions_latest_id_table, "counter").num) SET
                    expiry_date = <datetime>$expiry_date
                    , record = encoding::base64::decode($record_data);
                COMMIT TRANSACTION;
            "#).bind(("sessions_table", self.sessions_table.clone()))
            .bind(("sessions_latest_id_table", self.sessions_latest_id_table.clone()))
            .bind(("expiry_date", datetime_string))
            .bind(("record_data", record_data))
        // println!("{:#?}", result_query);
            .await
            .map_err(surreal_to_ts_error_conv)?
            .take((1, "id")).map_err(surreal_to_ts_error_conv)?;
        match result {
            Some(record_id) => {
                let SurrealId::Number(number) = record_id.id;
                record.id.0 = number.into()
            }
            , None => return Err(session_store::Error::Backend("Did not get ID for new record".into()))
        }
        Ok(())
    }
    
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let surrealdb_record: DatabaseRecord = record.into();
        let id_i64: i64 = match record.id.0.try_into() {
            Ok(number) => number
            , Err(e) => return Err(session_store::Error::Backend(e.to_string()))
        };
        let result = self.client
            .update::<Option<DatabaseRecord>>((&self.sessions_table, id_i64))
            .content(surrealdb_record)
            .await;
        match result {
            Ok(result_inner) => {
                match result_inner {
                    Some(_db_record) => Ok(())
                    , None => Err(session_store::Error::Backend("No record was updated".into()))
                }
            }
            , Err(e) => {
                println!("{}", e);
                Err(session_store::Error::Backend(e.to_string()))
            }
        }
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let mut result_obj = self.client.query(r#"
            select
                record
                , expiry_date
            from type::thing($table,$id)
            where
                expiry_date > time::now()
            "#).bind(("table", self.sessions_table.clone()))
            .bind(("id", session_id.0))
            .await.map_err(surreal_to_ts_error_conv)?;
        // println!("{:#?}", result_obj);
        let result: Option<DatabaseRecord> = result_obj
            .take(0)
            .map_err(surreal_to_ts_error_conv)?;
        match result {
            Some(data) => {
                let mut prelim_record: Record = data.into();
                prelim_record.id = session_id.clone();
                Ok(Some(prelim_record))
            }
            , None => Ok(None)
        }
    }
    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let id_i64: i64 = match session_id.0.try_into() {
            Ok(number) => number
            , Err(e) => return Err(session_store::Error::Backend(e.to_string()))
        };
        self.client
            .delete::<Option<DatabaseRecord>>((&self.sessions_table, id_i64))
            .await
            .map_err(surreal_to_ts_error_conv)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let store = SurrealdbStore::new_from_env().await.unwrap();
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
    let result = store.create(&mut my_record).await;
    match result {
        Err(e) => println!("Could not create record. Error is: {}", e)
        , Ok(()) => println!("Record creation successfull. ID is: {}", my_record.id.0)
    };
    let result = store.load(&my_record.id).await;
    match result {
        Err(e) => println!("Could not load record. Error is: {}", e)
        , Ok(record_option) => {
            match record_option{
                Some(record) => println!("Record loaded successfull. Data is: {:#?}", record)
                , None => println!("Load was successfull but no data was returned")
            }
        }
    };
    my_record.data.insert("test_key_2".into(), json!("test_value_2"));
    let result = store.save(&my_record).await;
    match result {
        Err(e) => println!("Could not load record. Error is: {}", e)
        , Ok(()) => println!("Record saved successfully")
    };
    let result = store.load(&my_record.id).await;
    match result {
        Err(e) => println!("Could not load record. Error is: {}", e)
        , Ok(record_option) => {
            match record_option{
                Some(record) => println!("Record loaded successfull. Data is: {:#?}", record)
                , None => println!("Load was successfull but no data was returned")
            }
        }
    };
    let result = store.delete(&my_record.id).await;
    match result {
        Err(e) => println!("Could not load record. Error is: {}", e)
        , Ok(()) => println!("Succesfully deleted record with id {}", my_record.id.0)
    };
    let result = store.load(&my_record.id).await;
    match result {
        Err(e) => println!("Could not load record. Error is: {}", e)
        , Ok(record_option) => {
            match record_option{
                Some(record) => println!("Record loaded successfull. Data is: {:#?}", record)
                , None => println!("Load was successfull but no data was returned")
            }
        }
    };
    ()
}