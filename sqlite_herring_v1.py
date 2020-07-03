#!/usr/bin/env python3

import pandas as pd
import sqlite3
import sys

conn = sqlite3.connect("herring_v1.db")  # creates db file

c = conn.cursor()  # cursor c

# Create tables
try:
    c.executescript(
        """DROP TABLE IF EXISTS Samples;
    DROP TABLE IF EXISTS Genotypes;
    DROP TABLE IF EXISTS Storage_info;
    DROP TABLE IF EXISTS Genetic_markers;
    DROP TABLE IF EXISTS Species_id_codes;
    DROP TABLE IF EXISTS Hafro_samples_info;

    CREATE TABLE Samples(
        Sample_id text PRIMARY KEY,
        Short_sid char(12) UNIQUE, --for input into genepop max 12 char
        Species_id_code int REFERENCES Species_id_codes(Species_id_code),
        Pop_name text,
        Maturity text, --"spawning" "feeding"
        FOREIGN KEY(Sample_id) REFERENCES Hafro_samples_info(Sample_id),
        FOREIGN KEY(Sample_id) REFERENCES Storage_info(Sample_id)
        );

    CREATE TABLE Genotypes(
        Sample_id text REFERENCES Samples(Sample_id),
        Marker_id text REFERENCES Genetic_markers(Marker_id),
        Gt text,
        PRIMARY KEY(Sample_id, Marker_id)
        );

    CREATE TABLE Storage_info(
        Sample_id text,
        Box text,
        Plate_id text,
        Well text,
        Sample_type text,  --"tissue", "scales", "otholit", "DNA"
        Matis_id int PRIMARY KEY
        );

    CREATE TABLE Genetic_markers(
        Marker_id text PRIMARY KEY,
        Short_mid text UNIQUE,
        Possible_genotypes text,
        Marker_type text, --affy
        Probe_sequence text, --{"forw":"ATCGCWACTW", "rev":"TGTRWCTG"}
        Marker_info text --"dinuclotype", "trinuclotide", "A/T"
        --Pcr_program text
        );

    CREATE TABLE Species_id_codes(
        Species_id_code int PRIMARY KEY,
        Species_latin text,
        Species_english text,
        Species_icelandic text
        );

    CREATE TABLE Hafro_samples_info(
        Sample_id text PRIMARY KEY,
        Date_time_collect date,
        Year_sampled int,
        Latitute real,
        Longitute real,
        Cruise_id text,
        Station text,
        Age int,
        Weight real,
        Maturity int,
        Length int,
        Sex int,
        Ship_id int,
        Location text,
        Sample_type text --"spawning", "feeding"
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
            csv_file = pd.read_csv(
                filename, sep=";", encoding="latin1"
            )  # latin1 Western Europe
            csv_file.to_sql(table, conn, if_exists="append", index=False)
    except sqlite3.Error:
        raise
    return None


def load_data(filenm, tb):
    check_table_exists(tb)
    csv_into_table(filenm, tb)
    return None


def get_gt_csv(file):
    """Queries and reshape to output a csv containing genotypes at each marker for 
    each individual with its short_id and population"""
    query = """SELECT Samples.Sample_id, Samples.Short_sid, Samples.Pop_name, 
    Genotypes.Marker_id, Genotypes.Gt
    FROM Samples, Genotypes
    WHERE Samples.Sample_id = Genotypes.Sample_id"""
    try:
        with conn:
            df = pd.read_sql_query(query, conn)
    except sqlite3.Error:
        raise
    df2 = df.pivot_table(  # long to wide format
        index=["Sample_id", "Short_sid", "Pop_name"],
        columns="Marker_id",
        values="Gt",  # str so need to adjust aggfunc
        aggfunc=lambda x: " ".join(
            x
        ),  # if several values for same index, will concatenate with space
    )
    df2.to_csv(file, sep=";")
    print(
        f"Pulling out all genotypes from all individuals x markers into csv {file}..."
    )
    return None


load_data("/mnt/c/Users/alexa/Matis/SQL/results/04_06_20/HerSNP_long.csv", "Genotypes")
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/herring/Species_id_codes.csv", "Species_id_codes"
)
load_data("/mnt/c/Users/alexa/Matis/SQL/data/herring/Samples.csv", "Samples")
load_data(
    "/mnt/c/Users/alexa/Matis/SQL/data/herring/Hafro_samples_info.csv",
    "Hafro_samples_info",
)

get_gt_csv("test_gt.csv")

conn.close()
