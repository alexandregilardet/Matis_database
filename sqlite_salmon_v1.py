#!/usr/bin/env python3

import pandas as pd
import sqlite3
import sys

conn = sqlite3.connect("salmon_v1.db")  # creates db file

c = conn.cursor()  # cursor c

# Create tables
try:
    c.executescript(
        """DROP TABLE IF EXISTS Samples;
    DROP TABLE IF EXISTS Genotypes;
    DROP TABLE IF EXISTS Snp_list;
    DROP TABLE IF EXISTS Snp_pos;
    DROP TABLE IF EXISTS Rivers;

    CREATE TABLE Samples(
        Sample_ID text PRIMARY KEY,
        Individual text UNIQUE,
        Species text,
        River text REFERENCES Rivers(River_code),
        Hybrid bool,
        Date_collected date,
        latitude real,
        longitude real
        );

    CREATE TABLE Genotypes(
        Sample_ID text REFERENCES Samples(Sample_ID),
        Probeset_ID text,
        Gt int NOT NULL,
        PRIMARY KEY(Sample_ID, Probeset_ID),
        FOREIGN KEY(Probeset_ID) REFERENCES Snp_list(Probeset_ID),
        FOREIGN KEY(Probeset_ID) REFERENCES Snp_pos(Probeset_ID)
        );

    CREATE TABLE Snp_list(
        Probeset_ID text PRIMARY KEY REFERENCES Snp_pos(Probeset_ID), 
        Affy_snp_ID text, 
        Conversion_type text, 
        Bestand_recommended int, 
        Cr real, 
        Minor_allele_frequency real, 
        H_w_p__value real, 
        Fld real, 
        Hom_fld real, 
        Het_so real, 
        Hom_ro real, 
        Nclus int, 
        N_aa int, 
        N_ab int, 
        N_bb int, 
        N_nc int, 
        Hemizygous int, 
        Hom_het int, 
        Best_probeset int, 
        Gender_metrics text, 
        Call_modified text
        );

    CREATE TABLE Snp_pos(
        Probeset_ID text PRIMARY KEY,
        Chr text,
        Pos bigint
        );

    CREATE TABLE Rivers(
        River_code text PRIMARY KEY,
        River_full_name text UNIQUE,
        Country text,
        Region text,
        Longitude real,
        Latitude real
        )"""
    )

except sqlite3.Error as err:
    print(err)
    raise  # reraise after print for Python to handle

conn.commit()


def check_table_exists(tbl):
    """Check wether tbl already exists from creation of database, otherwise it would create
    an additional one from the csv indexes"""
    c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )  # print all tables' names
    names = c.fetchall()
    print(f"Tables in the database are {names}")
    if (tbl,) in names:
        pass
    else:
        sys.exit(f"Something wrong with table {tbl}, it doesn't already exist")
    return None


def csv_into_table(filename, table):
    """Add csv into the database, csv should have columns named as the table in database,
    give table argument as a string"""
    try:
        with conn:
            csv_file = pd.read_csv(filename, sep=";")
            csv_file.to_sql(table, conn, if_exists="append", index=False)
    except sqlite3.Error:
        raise
    return None


def load_data(filenm, tb):
    check_table_exists(tb)
    csv_into_table(filenm, tb)
    return None


load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/rivers/rivers_to_import_sql.csv",
    "Rivers",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/snp_list/SsaTrack_Island_1-8_SNPlist_modified_sql_import.csv",
    "Snp_list",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/snp_pos/Track_SNP_Pos.csv",
    "Snp_pos",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/calls_mod/SsaTrack_Island_1-8.calls_mod.csv",
    "Genotypes",
)
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/salmon_sql_data/samples/salmon_beta_individuals.csv",
    "Samples",
)

conn.close()

# parse original data from CIGENE
