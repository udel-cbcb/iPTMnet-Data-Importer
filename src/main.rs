extern crate clap;
extern crate csv;
extern crate pbr;
extern crate postgres;
extern crate serde;
extern crate simplelog;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use postgres::{Connection, TlsMode};
use simplelog::*;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use pbr::ProgressBar;
use clap::{App, Arg};

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct MvEntry {
    IPTM_ENTRY_ID: i64,
    IPTM_ENTRY_CODE: String,
    IPTM_ENTRY_TYPE: String,
    IPTM_ENTRY_SYMBOL: String,
    UNIPROT_ID: String,
    PROTEIN_NAME: String,
    GENE_NAME: String,
    PROTEIN_SYNONYMS: String,
    GENE_SYNONYMS: String,
    DEFINITION: String,
    CATEGORY: String,
    IS_REVIEWED: String,
    TAXON_CODE: String,
    TAXON_SPECIES: String,
    TAXON_COMMON: String,
    NOTE: String,
    SITES: String,
    XREF: String,
    #[serde(deserialize_with = "csv::invalid_option")] NUM_ENZYME: Option<i64>,
    #[serde(deserialize_with = "csv::invalid_option")] NUM_SUBSTRATE: Option<i64>,
    #[serde(deserialize_with = "csv::invalid_option")] NUM_PPI: Option<i64>,
    #[serde(deserialize_with = "csv::invalid_option")] NUM_SITE: Option<i64>,
    #[serde(deserialize_with = "csv::invalid_option")] NUM_FORM: Option<i64>,
    ROLE_AS_ENZYME: String,
    ROLE_AS_SUBSTRATE: String,
    ROLE_AS_PPI: String,
    #[serde(deserialize_with = "csv::invalid_option")] WEIGHT: Option<i64>,
    LIST_AS_SUBSTRATE: String,
    LIST_AS_ENZYME: String,
    HAS_OVERLAP_PTM: String,
    PROTEIN_SYN: String,
    GENE_SYN: String,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct MvEvent {
    IPTM_EVENT_ID: i64,
    SUB_FORM_CODE: String,
    SUB_CODE: String,
    SUB_TYPE: String,
    SUB_UNIPROT_ID: String,
    SUB_SYMBOL: String,
    SUB_TAXON_CODE: String,
    SUB_TAXON_COMMON: String,
    SUB_SITES: String,
    SUB_XREF: String,
    ENZ_FORM_CODE: String,
    ENZ_CODE: String,
    ENZ_TYPE: String,
    ENZ_UNIPROT_ID: String,
    ENZ_SYMBOL: String,
    ENZ_TAXON_CODE: String,
    ENZ_TAXON_COMMON: String,
    ENZ_SITES: String,
    ENZ_XREF: String,
    EVENT_NAME: String,
    EVENT_LABEL: String,
    SOURCE_LABEL: String,
    IS_AUTO_GENERATED: String,
    RESIDUE: String,
    #[serde(deserialize_with = "csv::invalid_option")] POSITION: Option<i64>,
    MODIFIER: String,
    NOTE: String,
    PMIDS: String,
    NUM_SUBSTRATES: String,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct MvEfip {
    PPI_EVENT_ID: i64,
    PTM_EVENT_ID: i64,
    IMPACT: String,
    PPI_SUB_CODE: String,
    PPI_SUB_TYPE: String,
    PPI_SUB_SYMBOL: String,
    PPI_SUB_TAXON_CODE: String,
    PPI_SUB_TAXON_COMMON: String,
    PPI_SUB_SITES: String,
    PPI_PR_CODE: String,
    PPI_PR_TYPE: String,
    PPI_PR_SYMBOL: String,
    PPI_PR_TAXON_CODE: String,
    PPI_PR_TAXON_COMMON: String,
    PPI_SOURCE_LABEL: String,
    PPI_NOTE: String,
    PPI_PMIDS: String,
    PTM_SUB_CODE: String,
    PTM_SUB_TYPE: String,
    PTM_SUB_SYMBOL: String,
    PTM_SUB_TAXON_CODE: String,
    PTM_SUB_TAXON_COMMON: String,
    PTM_SUB_SITES: String,
    PTM_ENZ_CODE: String,
    PTM_ENZ_TYPE: String,
    PTM_ENZ_SYMBOL: String,
    PTM_ENZ_TAXON_CODE: String,
    PTM_ENZ_TAXON_COMMON: String,
    PTM_EVENT_NAME: String,
    PTM_EVENT_LABEL: String,
    PTM_RESIDUE: String,
    #[serde(deserialize_with = "csv::invalid_option")] PTM_POSITION: Option<i64>,
    PTM_SOURCE_LABEL: String,
    PTM_NOTE: String,
    PTM_PMIDS: String,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct MvProteo {
    SUB_CODE: String,
    SUB_TYPE: String,
    SUB_SYMBOL: String,
    SUB_SITES: String,
    SUB_XREF: String,
    ENZ_CODE: String,
    ENZ_TYPE: String,
    ENZ_SYMBOL: String,
    ENZ_SITES: String,
    ENZ_XREF: String,
    SITES: String,
    EVENT_NAME: String,
    EVENT_LABEL: String,
    SOURCE_LABEL: String,
    IS_AUTO_GENERATED: String,
    MODIFIER: String,
    PMIDS: String,
}

fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Info, Config::default()).unwrap(),
    ]).unwrap();

    let matches = App::new("iPTMnet data importer")
        .version("1.0")
        .author("Sachin Gavali. <saching@ude.edu>")
        .about("Imports data from CSV files into postgres database")
        .arg(
            Arg::with_name("HOST")
                .long("host")
                .help("The address of postgres database. Default - localhost")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("PORT")
                .long("port")
                .help("The port on which postgres database is running. Default - 5432 ")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("USERNAME")
                .long("user")
                .help("Username of the user that owns iptmnet database. Default - postgres")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("PASSWORD")
                .long("pass")
                .help("Password of the user that owns iptmnet database. Default - postgres")
                .takes_value(true),
        )
        .get_matches();

    let mut username = "postgres";
    let mut password = "postgres";
    let mut host = "localhost";
    let mut port = "5432";
    let database = "iptmnet";

    if matches.is_present("HOST") {
        host = matches.value_of("HOST").unwrap();
    }

    if matches.is_present("PORT") {
        port = matches.value_of("PORT").unwrap();
    }

    if matches.is_present("USERNAME") {
        username = matches.value_of("USERNAME").unwrap();
    }

    if matches.is_present("PASSWORD") {
        password = matches.value_of("PASSWORD").unwrap();
    }

    let connection_string = format!(
        "postgres://{username}:{password}@{host}:{port}/{database}",
        username = username,
        password = password,
        host = host,
        port = port,
        database = database
    );

    info!(
        "{}",
        format!(
            "Connecting to database at - {}.",
            connection_string.as_str()
        )
    );
    let conn = Connection::connect(connection_string.as_str(), TlsMode::None).unwrap();

    //START the transaction
    let start_transaction_result = conn.execute("BEGIN;", &[]);
    match start_transaction_result {
        Ok(_) => info!("STARTED TRANSACTION"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_ENTRY
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_ENTRY;", &[]);
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_EVENT
    let drop_mv_event_result = conn.execute("DROP TABLE IF EXISTS MV_EVENT;", &[]);
    match drop_mv_event_result {
        Ok(_) => info!("DROPPED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    //drop table MV_EVENT
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_EFIP;", &[]);
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }

    
    //create table MV_ENTRY
    create_mv_entry(&conn);

    //create table MV_EVENT
    create_mv_event(&conn);

    //create table MV_EFIP
    create_mv_efip(&conn);

    //create table MV_PROTEO
    create_mv_proteo(&conn);

    //populate mv_entry
    populate_mv_entry(&conn);

    //populate mv event
    populate_mv_event(&conn);

    //populate mv efip
    populate_mv_efip(&conn);
    
    //populate mv_proteo
    populate_mv_proteo(&conn);

    //END the transaction
    let end_transaction_result = conn.execute("COMMIT;", &[]);
    match end_transaction_result {
        Ok(_) => info!("END TRANSACTION"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn create_mv_entry(conn: &Connection) {
    let create_mv_entry_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_ENTRY
        (
            IPTM_ENTRY_ID BIGINT NOT NULL,
            IPTM_ENTRY_CODE VARCHAR(25) NOT NULL,
            IPTM_ENTRY_TYPE VARCHAR(10) NOT NULL,
            IPTM_ENTRY_SYMBOL VARCHAR(4000),
            UNIPROT_ID VARCHAR(50),
            PROTEIN_NAME VARCHAR(200),
            GENE_NAME VARCHAR(50),
            PROTEIN_SYNONYMS TEXT,
            GENE_SYNONYMS TEXT,
            DEFINITION TEXT,
            CATEGORY VARCHAR(25),
            IS_REVIEWED CHAR(1),
            TAXON_CODE VARCHAR(25),
            TAXON_SPECIES VARCHAR(200),
            TAXON_COMMON VARCHAR(100),
            NOTE TEXT,
            SITES TEXT,
            XREF VARCHAR(25),
            NUM_ENZYME BIGINT,
            NUM_SUBSTRATE BIGINT,
            NUM_PPI BIGINT,
            NUM_SITE BIGINT,
            NUM_FORM BIGINT,
            ROLE_AS_ENZYME CHAR(1),
            ROLE_AS_SUBSTRATE CHAR(1),
            ROLE_AS_PPI CHAR(1),
            WEIGHT BIGINT,
            LIST_AS_SUBSTRATE VARCHAR(25),
            LIST_AS_ENZYME VARCHAR(25),
            HAS_OVERLAP_PTM CHAR(1),
            PROTEIN_SYN VARCHAR(4000),
            GENE_SYN VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_entry_result {
        Ok(_) => info!("CREATED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_entry(conn: &Connection) {
    // Read mv_entry_exported.csv
    let file = File::open("./mv_entry_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let count: u64 = buf_reader.lines().count() as u64;

    let file = File::open("./mv_entry_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    info!("POPULATING MV_ENTRY");

    let mut pb = ProgressBar::new(count - 1);
    pb.format("╢▌▌░╟");

    for result in rdr.deserialize() {
        //read the entry
        let mv_entry: MvEntry = result.unwrap();
        //insert into postgres
        let insert_result = conn.execute("INSERT INTO MV_ENTRY 
                        (
                            IPTM_ENTRY_ID,
                            IPTM_ENTRY_CODE,
                            IPTM_ENTRY_TYPE,
                            IPTM_ENTRY_SYMBOL,
                            UNIPROT_ID,
                            PROTEIN_NAME,
                            GENE_NAME,
                            PROTEIN_SYNONYMS,
                            GENE_SYNONYMS,
                            DEFINITION,
                            CATEGORY,
                            IS_REVIEWED,
                            TAXON_CODE,
                            TAXON_SPECIES,
                            TAXON_COMMON,
                            NOTE,
                            SITES,
                            XREF,
                            NUM_ENZYME,
                            NUM_SUBSTRATE,
                            NUM_PPI,
                            NUM_SITE,
                            NUM_FORM,
                            ROLE_AS_ENZYME,
                            ROLE_AS_SUBSTRATE,
                            ROLE_AS_PPI,
                            WEIGHT,
                            LIST_AS_SUBSTRATE,
                            LIST_AS_ENZYME,
                            HAS_OVERLAP_PTM,
                            PROTEIN_SYN,
                            GENE_SYN
                        )                     
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)",
                 &[&mv_entry.IPTM_ENTRY_ID,
                  &mv_entry.IPTM_ENTRY_CODE,
                  &mv_entry.IPTM_ENTRY_TYPE,
                  &mv_entry.IPTM_ENTRY_SYMBOL,
                  &mv_entry.UNIPROT_ID,
                  &mv_entry.PROTEIN_NAME,
                  &mv_entry.GENE_NAME,
                  &mv_entry.PROTEIN_SYNONYMS,
                  &mv_entry.GENE_SYNONYMS,
                  &mv_entry.DEFINITION,
                  &mv_entry.CATEGORY,
                  &mv_entry.IS_REVIEWED,
                  &mv_entry.TAXON_CODE,
                  &mv_entry.TAXON_SPECIES,
                  &mv_entry.TAXON_COMMON,
                  &mv_entry.NOTE,
                  &mv_entry.SITES,
                  &mv_entry.XREF,
                  &mv_entry.NUM_ENZYME,
                  &mv_entry.NUM_SUBSTRATE,
                  &mv_entry.NUM_PPI,
                  &mv_entry.NUM_SITE,
                  &mv_entry.NUM_FORM,
                  &mv_entry.ROLE_AS_ENZYME,
                  &mv_entry.ROLE_AS_SUBSTRATE,
                  &mv_entry.ROLE_AS_PPI,
                  &mv_entry.WEIGHT,
                  &mv_entry.LIST_AS_SUBSTRATE,
                  &mv_entry.LIST_AS_ENZYME,
                  &mv_entry.HAS_OVERLAP_PTM,
                  &mv_entry.PROTEIN_SYN,
                  &mv_entry.GENE_SYN                           
                  ]);

        match insert_result {
            Ok(_) => {
                pb.inc();
            }
            Err(err) => {
                error!("{}", err);
                error!("{:?}", &mv_entry);
                std::process::exit(1);
            }
        }
    }
}

fn create_mv_event(conn: &Connection) {
    let create_mv_event_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_EVENT
        (
            IPTM_EVENT_ID BIGINT NOT NULL,
            SUB_FORM_CODE VARCHAR(25),
            SUB_CODE VARCHAR(25),
            SUB_TYPE VARCHAR(10),
            SUB_UNIPROT_ID VARCHAR(50),
            SUB_SYMBOL VARCHAR(4000),
            SUB_TAXON_CODE VARCHAR(25),
            SUB_TAXON_COMMON VARCHAR(100),
            SUB_SITES TEXT,
            SUB_XREF VARCHAR(25),
            ENZ_FORM_CODE VARCHAR(25),
            ENZ_CODE VARCHAR(25),
            ENZ_TYPE VARCHAR(10),
            ENZ_UNIPROT_ID VARCHAR(50),
            ENZ_SYMBOL VARCHAR(4000),
            ENZ_TAXON_CODE VARCHAR(25),
            ENZ_TAXON_COMMON VARCHAR(100),
            ENZ_SITES TEXT,
            ENZ_XREF VARCHAR(25),
            EVENT_NAME VARCHAR(50),
            EVENT_LABEL VARCHAR(10),
            SOURCE_LABEL VARCHAR(10),
            IS_AUTO_GENERATED CHAR(1),
            RESIDUE VARCHAR(1),
            POSITION BIGINT,
            MODIFIER VARCHAR(50),
            NOTE TEXT,
            PMIDS TEXT,
            NUM_SUBSTRATES VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_event_result {
        Ok(_) => info!("CREATED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_event(conn: &Connection) {
    // Read mv_event_exported.csv
    let file = File::open("./mv_event_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let count: u64 = buf_reader.lines().count() as u64;

    let file = File::open("./mv_event_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    info!("POPULATING MV_EVENT");

    let mut pb = ProgressBar::new(count - 1);
    pb.format("╢▌▌░╟");

    for result in rdr.deserialize() {
        //read the entry
        let mv_event: MvEvent = result.unwrap();
        //insert into postgres
        let insert_result = conn.execute("INSERT INTO MV_EVENT 
                        (
                            IPTM_EVENT_ID,
                            SUB_FORM_CODE,
                            SUB_CODE,
                            SUB_TYPE,
                            SUB_UNIPROT_ID,
                            SUB_SYMBOL,
                            SUB_TAXON_CODE,
                            SUB_TAXON_COMMON,
                            SUB_SITES,
                            SUB_XREF,
                            ENZ_FORM_CODE,
                            ENZ_CODE,
                            ENZ_TYPE,
                            ENZ_UNIPROT_ID,
                            ENZ_SYMBOL,
                            ENZ_TAXON_CODE,
                            ENZ_TAXON_COMMON,
                            ENZ_SITES,
                            ENZ_XREF,
                            EVENT_NAME,
                            EVENT_LABEL,
                            SOURCE_LABEL,
                            IS_AUTO_GENERATED,
                            RESIDUE,
                            POSITION,
                            MODIFIER,
                            NOTE,
                            PMIDS,
                            NUM_SUBSTRATES
                        )                     
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)",
                 &[&mv_event.IPTM_EVENT_ID,
                  &mv_event.SUB_FORM_CODE,
                  &mv_event.SUB_CODE,
                  &mv_event.SUB_TYPE,
                  &mv_event.SUB_UNIPROT_ID,
                  &mv_event.SUB_SYMBOL,
                  &mv_event.SUB_TAXON_CODE,
                  &mv_event.SUB_TAXON_COMMON,
                  &mv_event.SUB_SITES,
                  &mv_event.SUB_XREF,
                  &mv_event.ENZ_FORM_CODE,
                  &mv_event.ENZ_CODE,
                  &mv_event.ENZ_TYPE,
                  &mv_event.ENZ_UNIPROT_ID,
                  &mv_event.ENZ_SYMBOL,
                  &mv_event.ENZ_TAXON_CODE,
                  &mv_event.ENZ_TAXON_COMMON,
                  &mv_event.ENZ_SITES,
                  &mv_event.ENZ_XREF,
                  &mv_event.EVENT_NAME,
                  &mv_event.EVENT_LABEL,
                  &mv_event.SOURCE_LABEL,
                  &mv_event.IS_AUTO_GENERATED,
                  &mv_event.RESIDUE,
                  &mv_event.POSITION,
                  &mv_event.MODIFIER,
                  &mv_event.NOTE,
                  &mv_event.PMIDS,
                  &mv_event.NUM_SUBSTRATES                           
                  ]);

        match insert_result {
            Ok(_) => {
                pb.inc();
            }
            Err(err) => {
                error!("{}", err);
                error!("{:?}", &mv_event);
                std::process::exit(1);
            }
        }
    }
}

fn create_mv_efip(conn: &Connection) {
    let create_mv_efip_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_EFIP
        (
            PPI_EVENT_ID BIGINT,
            PTM_EVENT_ID BIGINT,
            IMPACT VARCHAR(50),
            PPI_SUB_CODE VARCHAR(25),
            PPI_SUB_TYPE VARCHAR(10),
            PPI_SUB_SYMBOL VARCHAR(4000),
            PPI_SUB_TAXON_CODE VARCHAR(25),
            PPI_SUB_TAXON_COMMON VARCHAR(100),
            PPI_SUB_SITES TEXT,
            PPI_PR_CODE VARCHAR(25),
            PPI_PR_TYPE VARCHAR(10),
            PPI_PR_SYMBOL VARCHAR(4000),
            PPI_PR_TAXON_CODE VARCHAR(25),
            PPI_PR_TAXON_COMMON VARCHAR(100),
            PPI_SOURCE_LABEL VARCHAR(10),
            PPI_NOTE TEXT,
            PPI_PMIDS TEXT,
            PTM_SUB_CODE VARCHAR(25),
            PTM_SUB_TYPE VARCHAR(10),
            PTM_SUB_SYMBOL VARCHAR(4000),
            PTM_SUB_TAXON_CODE VARCHAR(25),
            PTM_SUB_TAXON_COMMON VARCHAR(100),
            PTM_SUB_SITES TEXT,
            PTM_ENZ_CODE VARCHAR(25),
            PTM_ENZ_TYPE VARCHAR(10),
            PTM_ENZ_SYMBOL VARCHAR(4000),
            PTM_ENZ_TAXON_CODE VARCHAR(25),
            PTM_ENZ_TAXON_COMMON VARCHAR(100),
            PTM_EVENT_NAME VARCHAR(50),
            PTM_EVENT_LABEL VARCHAR(10),
            PTM_RESIDUE VARCHAR(1),
            PTM_POSITION BIGINT,
            PTM_SOURCE_LABEL VARCHAR(10),
            PTM_NOTE TEXT,
            PTM_PMIDS TEXT
        )",
        &[],
    );

    match create_mv_efip_result {
        Ok(_) => info!("CREATED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_efip(conn: &Connection) {
    // Read mv_efip_exported.csv
    let file = File::open("./mv_efip_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let count: u64 = buf_reader.lines().count() as u64;

    let file = File::open("./mv_efip_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    info!("POPULATING MV_EFIP");

    let mut pb = ProgressBar::new(count - 1);
    pb.format("╢▌▌░╟");

    for result in rdr.deserialize() {
        //read the entry
        let mv_efip: MvEfip = result.unwrap();
        //insert into postgres
        let insert_result = conn.execute("INSERT INTO MV_EFIP 
                        (
                            PPI_EVENT_ID,
                            PTM_EVENT_ID,
                            IMPACT,
                            PPI_SUB_CODE,
                            PPI_SUB_TYPE,
                            PPI_SUB_SYMBOL,
                            PPI_SUB_TAXON_CODE,
                            PPI_SUB_TAXON_COMMON,
                            PPI_SUB_SITES,
                            PPI_PR_CODE,
                            PPI_PR_TYPE,
                            PPI_PR_SYMBOL,
                            PPI_PR_TAXON_CODE,
                            PPI_PR_TAXON_COMMON,
                            PPI_SOURCE_LABEL,
                            PPI_NOTE,
                            PPI_PMIDS,
                            PTM_SUB_CODE,
                            PTM_SUB_TYPE,
                            PTM_SUB_SYMBOL,
                            PTM_SUB_TAXON_CODE,
                            PTM_SUB_TAXON_COMMON,
                            PTM_SUB_SITES,
                            PTM_ENZ_CODE,
                            PTM_ENZ_TYPE,
                            PTM_ENZ_SYMBOL,
                            PTM_ENZ_TAXON_CODE,
                            PTM_ENZ_TAXON_COMMON,
                            PTM_EVENT_NAME,
                            PTM_EVENT_LABEL,
                            PTM_RESIDUE,
                            PTM_POSITION,
                            PTM_SOURCE_LABEL,
                            PTM_NOTE,
                            PTM_PMIDS
                        )                     
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35)",
                 &[&mv_efip.PPI_EVENT_ID,
                  &mv_efip.PTM_EVENT_ID,
                  &mv_efip.IMPACT,
                  &mv_efip.PPI_SUB_CODE,
                  &mv_efip.PPI_SUB_TYPE,
                  &mv_efip.PPI_SUB_SYMBOL,
                  &mv_efip.PPI_SUB_TAXON_CODE,
                  &mv_efip.PPI_SUB_TAXON_COMMON,
                  &mv_efip.PPI_SUB_SITES,
                  &mv_efip.PPI_PR_CODE,
                  &mv_efip.PPI_PR_TYPE,
                  &mv_efip.PPI_PR_SYMBOL,
                  &mv_efip.PPI_PR_TAXON_CODE,
                  &mv_efip.PPI_PR_TAXON_COMMON,
                  &mv_efip.PPI_SOURCE_LABEL,
                  &mv_efip.PPI_NOTE,
                  &mv_efip.PPI_PMIDS,
                  &mv_efip.PTM_SUB_CODE,
                  &mv_efip.PTM_SUB_TYPE,
                  &mv_efip.PTM_SUB_SYMBOL,
                  &mv_efip.PTM_SUB_TAXON_CODE,
                  &mv_efip.PTM_SUB_TAXON_COMMON,
                  &mv_efip.PTM_SUB_SITES,
                  &mv_efip.PTM_ENZ_CODE,
                  &mv_efip.PTM_ENZ_TYPE,
                  &mv_efip.PTM_ENZ_SYMBOL,
                  &mv_efip.PTM_ENZ_TAXON_CODE,
                  &mv_efip.PTM_ENZ_TAXON_COMMON,
                  &mv_efip.PTM_EVENT_NAME,
                  &mv_efip.PTM_EVENT_LABEL,
                  &mv_efip.PTM_RESIDUE,
                  &mv_efip.PTM_POSITION,
                  &mv_efip.PTM_SOURCE_LABEL,
                  &mv_efip.PTM_NOTE,
                  &mv_efip.PTM_PMIDS                                       
                  ]);

        match insert_result {
            Ok(_) => {
                pb.inc();
            }
            Err(err) => {
                error!("{}", err);
                error!("{:?}", &mv_efip);
                std::process::exit(1);
            }
        }
    }
}

fn create_mv_proteo(conn: &Connection) {
    let create_mv_entry_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS MV_PROTEO
        (
            SUB_CODE VARCHAR(25),
            SUB_TYPE VARCHAR(10),
            SUB_SYMBOL VARCHAR(4000),
            SUB_SITES VARCHAR(4000),
            SUB_XREF VARCHAR(25),
            ENZ_CODE VARCHAR(25),
            ENZ_TYPE VARCHAR(10),
            ENZ_SYMBOL VARCHAR(4000),
            ENZ_SITES VARCHAR(4000),
            ENZ_XREF VARCHAR(25),
            SITES VARCHAR(4000),
            EVENT_NAME VARCHAR(50) NOT NULL,
            EVENT_LABEL VARCHAR(10) NOT NULL,
            SOURCE_LABEL VARCHAR(10) NOT NULL,
            IS_AUTO_GENERATED CHAR(1) NOT NULL,
            MODIFIER VARCHAR(50),
            PMIDS VARCHAR(4000)
        )",
        &[],
    );

    match create_mv_entry_result {
        Ok(_) => info!("CREATED TABLE MV_PROTEO"),
        Err(val) => {
            error!("{}", val);
            std::process::exit(1);
        }
    }
}

fn populate_mv_proteo(conn: &Connection) {
    // Read mv_efip_exported.csv
    let file = File::open("./mv_proteo_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let count: u64 = buf_reader.lines().count() as u64;

    let file = File::open("./mv_proteo_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    info!("POPULATING MV_PROTEO");

    let mut pb = ProgressBar::new(count - 1);
    pb.format("╢▌▌░╟");

    for result in rdr.deserialize() {
        //read the entry
        let mv_proteo: MvProteo = result.unwrap();
        //insert into postgres
        let insert_result = conn.execute(
            "INSERT INTO MV_PROTEO 
                        (
                            SUB_CODE,
                            SUB_TYPE,
                            SUB_SYMBOL,
                            SUB_SITES,
                            SUB_XREF,
                            ENZ_CODE,
                            ENZ_TYPE,
                            ENZ_SYMBOL,
                            ENZ_SITES,
                            ENZ_XREF,
                            SITES,
                            EVENT_NAME,
                            EVENT_LABEL,
                            SOURCE_LABEL,
                            IS_AUTO_GENERATED,
                            MODIFIER,
                            PMIDS
                        )                     
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            &[
                &mv_proteo.SUB_CODE,
                &mv_proteo.SUB_TYPE,
                &mv_proteo.SUB_SYMBOL,
                &mv_proteo.SUB_SITES,
                &mv_proteo.SUB_XREF,
                &mv_proteo.ENZ_CODE,
                &mv_proteo.ENZ_TYPE,
                &mv_proteo.ENZ_SYMBOL,
                &mv_proteo.ENZ_SITES,
                &mv_proteo.ENZ_XREF,
                &mv_proteo.SITES,
                &mv_proteo.EVENT_NAME,
                &mv_proteo.EVENT_LABEL,
                &mv_proteo.SOURCE_LABEL,
                &mv_proteo.IS_AUTO_GENERATED,
                &mv_proteo.MODIFIER,
                &mv_proteo.PMIDS,
            ],
        );

        match insert_result {
            Ok(_) => {
                pb.inc();
            }
            Err(err) => {
                error!("{}", err);
                error!("{:?}", &mv_proteo);
                std::process::exit(1);
            }
        }
    }
}
