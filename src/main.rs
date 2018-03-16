extern crate postgres;
extern crate serde;
extern crate csv;
extern crate simplelog;
extern crate pbr;


#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use postgres::{Connection, TlsMode};
use simplelog::*;
use std::process;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use pbr::ProgressBar;

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
    NUM_ENZYME: i64,
    NUM_SUBSTRATE: i64,
    NUM_PPI: i64,
    NUM_SITE: i64,
    NUM_FORM: i64,
    ROLE_AS_ENZYME: String,
    ROLE_AS_SUBSTRATE: String,
    ROLE_AS_PPI: String,
    WEIGHT: i64,
    LIST_AS_SUBSTRATE: String,
    LIST_AS_ENZYME: String,
    HAS_OVERLAP_PTM: String,
    PROTEIN_SYN: String,
    GENE_SYN: String
}

fn main() {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info,Config::default()).unwrap(),
        ]
    ).unwrap();
    let username = "postgres";
    let password = "postgres";
    let host= "localhost";
    let port = "5432";    
    let database = "iptmnet";
    let connection_string = format!("postgres://{username}:{password}@{host}:{port}/{database}",
                                     username=username,
                                     password=password,
                                     host=host,
                                     port=port,
                                     database=database);
    
    info!("{}",format!("Connecting to database at - {}.",connection_string.as_str()));
    let conn = Connection::connect(connection_string.as_str(),TlsMode::None).unwrap();

    //START the transaction
    let start_transaction_result = conn.execute("BEGIN;",&[]);
    match start_transaction_result {
        Ok(_) => info!("STARTED TRANSACTION"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    //drop table MV_ENTRY
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_ENTRY;",&[]);  
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    //drop table MV_EVENT
    let drop_mv_event_result = conn.execute("DROP TABLE IF EXISTS MV_EVENT;",&[]);  
    match drop_mv_event_result {
        Ok(_) => info!("DROPPED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    //drop table MV_EVENT
    let drop_mv_entry_result = conn.execute("DROP TABLE IF EXISTS MV_EFIP;",&[]);  
    match drop_mv_entry_result {
        Ok(_) => info!("DROPPED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }


    //create table MV_ENTRY
    let create_mv_entry_result = conn.execute("CREATE TABLE IF NOT EXISTS MV_ENTRY
        (
            IPTM_ENTRY_ID BIGINT NOT NULL,
            IPTM_ENTRY_CODE VARCHAR(25) NOT NULL,
            IPTM_ENTRY_TYPE VARCHAR(10) NOT NULL,
            IPTM_ENTRY_SYMBOL VARCHAR(4000),
            UNPROT_ID VARCHAR(50),
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
        )", &[]);

    match create_mv_entry_result {
        Ok(_) => info!("CREATED TABLE MV_ENTRY"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    //create table MV_EVENT
    let create_mv_event_result = conn.execute("CREATE TABLE IF NOT EXISTS MV_EVENT
        (
            IPTM_EVENT_ID BIGINT NOT NULL,
            SUB_FORM_CODE VARCHAR(25),
            SUB_CODE VARCHAR(25),
            SUB_TYPE VARCHAR(10),
            SUB_UNIPROT_ID VARCHAR(50),
            SUB_SYMBOL VARCHAR(4000),
            SUB_TAXON_CODE VARCHAR(25),
            SUB_TAXON_CODE_COMMON VARCHAR(100),
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
        )", &[]);

    match create_mv_event_result {
        Ok(_) => info!("CREATED TABLE MV_EVENT"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    //create table MV_EFIP
    let create_mv_efip_result = conn.execute("CREATE TABLE IF NOT EXISTS MV_EFIP
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
        )", &[]);

    match create_mv_efip_result {
        Ok(_) => info!("CREATED TABLE MV_EFIP"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

    // Read mv_entry_exported.csv
    let file = File::open("./mv_entry_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let count: u64 = buf_reader.lines().count() as u64;

    let file = File::open("./mv_entry_export.csv").unwrap();
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    info!("POPULATING MV_ENTRY");

    let mut pb = ProgressBar::new(count);
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
                            UNPROT_ID,
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
            Ok(val) => {
                pb.inc();            
            },
            Err(err) => {
                error!("{}",err);
                std::process::exit(1);
            }
        }

    }

    //END the transaction
    let end_transaction_result = conn.execute("COMMIT;",&[]);
    match end_transaction_result {
        Ok(_) => info!("END TRANSACTION"),
        Err(val) => {
            error!("{}",val);
            std::process::exit(1);
        }
    }

}