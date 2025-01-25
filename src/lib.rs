use anyhow;
use anyhow::Context;
use surrealdb;
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
};
use tower_sessions::{
    cookie::time::format_description::well_known::{
        Rfc3339
        , Iso8601
        , iso8601::{
            TimePrecision
            , Config
            , EncodedConfig
        }
    }
    , session_store::Error::{
        Backend
        , Encode
        , Decode
    }
    , session_store
};
use chrono;
use serde::{Deserialize, Serialize};
use std::{
    env::var
    , num::NonZeroU8
    , fmt::Debug
};
use async_trait::async_trait;
use base64::{
    prelude::BASE64_STANDARD_NO_PAD
    , Engine
};

#[cfg(test)]
mod tests;

const FORMAT_CONFIG: EncodedConfig = Config::DEFAULT.set_time_precision(
    TimePrecision::Second{decimal_digits: NonZeroU8::new(6)}
).encode();

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

impl TryFrom<&Record> for DatabaseRecord {
    type Error = session_store::Error;

    fn try_from(record: &Record) -> session_store::Result<Self> {
        let interim_datetime_string = record.expiry_date.format(&Rfc3339)
            .map_err(|e| Encode(e.to_string()))?;
        let chrono_datetime = interim_datetime_string.parse::<chrono::DateTime<chrono::offset::Utc>>()
            .map_err(|e| Encode(e.to_string()))?;

        Ok(Self {
            record: rmp_serde::to_vec(record)
                .map_err(|e| Encode(e.to_string()))?
            , expiry_date: Datetime::from(chrono_datetime)
        })
    }
}

impl TryFrom<DatabaseRecord> for Record {
    type Error = session_store::Error;

    fn try_from(database_record: DatabaseRecord) -> session_store::Result<Record> {
        rmp_serde::from_slice(&database_record.record).map_err(|e| Decode(e.to_string()))
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

    /// Enables creating a new SurrealdbStore from a supplied Surreal
    /// struct.
    /// ```
    /// use anyhow;
    /// use surrealdb::{
    ///     Surreal
    ///     , engine::local::{Db, Mem}
    /// };
    /// use tower_sessions_surrealdb_store::SurrealdbStore;
    /// 
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()>{
    ///     let my_surreal: Surreal<Db> = Surreal::init();
    ///     let my_client = my_surreal.connect::<Mem>(()).await?;
    ///     let my_surreal_store = SurrealdbStore::new(
    ///         my_surreal
    ///         , "sessions_table".into()
    ///         , "sessions_latest_id_table".into()
    ///     );
    ///     Ok(())
    /// }
    /// ```

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
    
    /// Creates the data model in the database to support the store.
    /// 
    /// Example code for memory database
    /// ```
    /// use anyhow;
    /// use surrealdb::{
    ///     Surreal
    ///     , engine::local::{Db, Mem}
    /// };
    /// use tower_sessions_surrealdb_store::SurrealdbStore;
    /// 
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()>{
    ///     let my_surreal: Surreal<Db> = Surreal::init();
    ///     let my_client = my_surreal.connect::<Mem>(()).await?;
    ///     let my_surreal_store = SurrealdbStore::new(
    ///         my_surreal
    ///         , "sessions_table".into()
    ///         , "sessions_latest_id_table".into()
    ///     ).await;
    ///     my_surreal_store.create_data_model().await?;
    ///     Ok(())
    /// }
    /// ```
    /// 
    /// Example code for rocksdb based database
    /// ```
    /// use anyhow;
    /// use surrealdb::engine::any::Any;
    /// use tower_sessions_surrealdb_store::SurrealdbStore;
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()>{
    ///     let my_surreal_store: SurrealdbStore<Any> = SurrealdbStore::new_from_nothing(
    ///         "ws".into()
    ///         , "localhost:8000".into()
    ///         , "root".into()
    ///         , "namespace".into()
    ///         , "database".into()
    ///         , "sessions".into()
    ///         , "sessions_latest_id".into()
    ///     ).await?;
    ///     my_surreal_store.create_data_model().await?;
    ///     Ok(())
    /// }
    /// ```

    pub async fn create_data_model(&self) -> anyhow::Result<()> {
        let creation_query = format!(r"
                BEGIN TRANSACTION;
                DEFINE TABLE IF NOT EXISTS {0} SCHEMAFULL;
                DEFINE FIELD IF NOT EXISTS id ON TABLE {0} TYPE int;
                DEFINE FIELD IF NOT EXISTS expiry_date ON TABLE {0} TYPE datetime;
                DEFINE FIELD IF NOT EXISTS record ON TABLE {0} TYPE bytes;
                COMMIT TRANSACTION;
            ", self.sessions_table);
        self.client.query(creation_query)
            .await?;
        Ok(())
    }
}

impl SurrealdbStore<Any> {

    /// Enables creating a SurrealdbStore<Any> instance from nothing.
    /// To use this please make sure you configure your env variable
    /// DB_PASSWORD to hold your password. Paswords in code is a
    /// big no no
    /// Note: Please pick appropriate values for anything else other
    /// than testing
    /// ```
    /// use anyhow;
    /// use surrealdb::engine::any::Any;
    /// use tower_sessions_surrealdb_store::SurrealdbStore;
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()>{
    ///     let my_surreal: SurrealdbStore<Any> = SurrealdbStore::new_from_nothing(
    ///         "ws".into()
    ///         , "localhost:8000".into()
    ///         , "root".into()
    ///         , "namespace".into()
    ///         , "database".into()
    ///         , "sessions".into()
    ///         , "sessions_latest_id_table".into()
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```

    pub async fn new_from_nothing(
        endpoint_type: String
        , endpoint_address: String
        , username: String
        , namespace: String
        , database: String
        , sessions_table: String
        , sessions_latest_id_table: String
    ) -> anyhow::Result<Self> {
        // Connect to the database
        let db_password = var("DB_PASSWORD").context("DB_PASSWORD env var not defined")?;

        let surreal_connection: Surreal<Any> = Surreal::init();
        surreal_connection.connect(format!("{endpoint_type}://{endpoint_address}")).await
            .context(format!("Could not connect to SurrealDB. Either the endpoint type was\
                wrong or the endpoint address was wrong.\n\
                Endpoint type was: {endpoint_type}\n\
                Endpoint address was {endpoint_address}"
            ))?;
        
        // Log into the database
        surreal_connection.signin(Root {
            username: username.as_str(),
            password: db_password.as_str(),
        }).await.context(format!("Username or password was wrong.\n\
            Username was: {username}\n\
            Can't print the password. Check it in your env var."
        ))?;
        
        // Select a namespace/database
        surreal_connection.use_ns(&namespace).use_db(&database).await
            .context(format!("Check that the names or the namespace and database are correct\n\
                that they exist.\n\
                Namespace was {namespace}.\n\
                Database was {database}"
            ))?;
        Ok(
            Self {
                client: surreal_connection
                , sessions_table: sessions_table
                , sessions_latest_id_table: sessions_latest_id_table
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
            .map_err(|e| Backend(e.to_string()))?
            .check()
            .map_err(|e| Backend(e.to_string()))?;
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
        let surrealdb_record: DatabaseRecord = record_reference.try_into()?;
        let datetime_string = record_reference.expiry_date
            .format(&Iso8601::<{FORMAT_CONFIG}>)
            .map_err(|e| Encode(e.to_string()))?;
        let record_data = BASE64_STANDARD_NO_PAD.encode(surrealdb_record.record);
        let query = format!(r#"
            BEGIN TRANSACTION;
            UPSERT type::thing("{0}", "counter") SET num += 1;
            CREATE type::thing("{1}", type::thing("{0}", "counter").num) SET
                expiry_date = <datetime>"{2}"
                , record = encoding::base64::decode("{3}");
            COMMIT TRANSACTION;"#
            , self.sessions_latest_id_table.clone()
            , self.sessions_table.clone()
            , datetime_string
            , record_data
        );
        let result: Option<RecordId> = self.client.query(query).await
            .map_err(|e| Backend(e.to_string()))?
            .take((1, "id")).map_err(|e | Backend(e.to_string()))?;
        let new_id = result.ok_or(Backend("Record was not created so no ID was returned".into()))?;
        let SurrealId::Number(number) = new_id.id;
        record.id.0 = number.into();
        Ok(())
    }
    
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let surrealdb_record: DatabaseRecord = record.try_into()?;
        let id_i64: i64 = record.id.0.try_into()
            .map_err(|_| Encode("ID was out of range for target data type of i64".into()))?;
        let result = self.client
            .update::<Option<DatabaseRecord>>((&self.sessions_table, id_i64))
            .content(surrealdb_record)
            .await;
        result.map_err(|e| Backend(e.to_string()))?
            .ok_or(Backend("No record was updated. Probably ID not found".into()))?;
        Ok(())
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
            .await.map_err(|e| Backend(e.to_string()))?;
        let result: Option<DatabaseRecord> = result_obj
            .take(0)
            .map_err(|e| Backend(e.to_string()))?;
        match result {
            Some(data) => {
                let mut prelim_record: Record = data.try_into()
                .map_err(|_| Decode(
                    "Database record could not be converted to type Record".into()
                ))?;
                prelim_record.id = session_id.clone();
                Ok(Some(prelim_record))
            }
            , None => Ok(None)
        }
    }
    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let id_i64: i64 = session_id.0.try_into().map_err(|_| Encode(
            "ID was out of range for target data type of i64".into()
        ))?;
        self.client
            .delete::<Option<DatabaseRecord>>((&self.sessions_table, id_i64))
            .await
            .map_err(|e| Backend(e.to_string()))?;
        Ok(())
    }
}