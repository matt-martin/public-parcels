from geopandas import GeoDataFrame

import duckdb
import math
import sys

if len(sys.argv) != 2:
    print("Please provide input directory as the only argument to this script")
    exit(1)

input_dir = sys.argv[1]

print(f"Reading data from {input_dir}")

geo_df = GeoDataFrame.from_file(input_dir)
geo_df = geo_df.to_wkt()
geo_df = geo_df.rename(
    {
        "ASMT": "apn",
        "ASMTWithDa": "apn_with_dash",
        "LandUse1": "land_use_code",
        "TRA": "tax_rate_area_code",
        "floor": "floor_number",
        "Shape_STAr": "shape_st_area",
        "Shape_STLe": "shape_st_length",
        "Acres": "acres",
        "Notes": "notes"
    },
    axis=1
)
# print(geo_df.columns)

# validate
con = duckdb.connect(database=':memory:')

apn_counts = con.execute("""
    SELECT
      COUNT(*) as total_row_count,
      SUM(IF(apn = replace(apn_with_dash, '-', ''), 1, 0)) as matching_row_count,
      COUNT(DISTINCT apn) as num_apns,
      COUNT(DISTINCT apn_with_dash) as num_apns_with_dash
    FROM geo_df
""").fetchdf()

total_row_count = apn_counts.loc[0, 'total_row_count']
matching_row_count = math.floor(apn_counts.loc[0, 'matching_row_count'])
mismatching_rows = total_row_count - matching_row_count
msg = f'Found {mismatching_rows} rows out of {total_row_count} rows where the APN and APN with dash columns do not match'
print(msg)
assert mismatching_rows == 0, msg


conflicting_rows = con.execute("""
    SELECT
      apn,
      array_agg(DISTINCT acres) as acres_list,
      array_agg(DISTINCT land_use_code) as land_use_code_list,
      array_agg(DISTINCT tax_rate_area_code) as tax_rate_area_code_list,
      array_agg(DISTINCT floor_number) as floor_number_list
    FROM geo_df
    GROUP BY apn
    HAVING
      len(acres_list) > 1
      OR len(land_use_code_list) > 1
      OR len(tax_rate_area_code_list) > 1
      OR len(floor_number_list) > 1
""").fetchdf()
num_conflicting_rows = len(conflicting_rows.index)
conflicts_msg = f'Found {num_conflicting_rows} rows out of {total_row_count} where info appears to conflict'
print(conflicts_msg)

# parcels_with_more_than_one_row = con.execute("""
#     SELECT
#       apn,
#       count(*) as num_rows
#     FROM geo_df
#     GROUP BY apn
#     HAVING
#       num_rows > 1
# """).fetch_arrow_table()
# print(con.fetchall())
#
# repeated_parcels = con.execute("""
#     SELECT geo_df.*
#     FROM geo_df
#     JOIN parcels_with_more_than_one_row
#     ON geo_df.apn = parcels_with_more_than_one_row.apn
#     ORDER BY geo_df.apn
# """).fetchdf()
# print(repeated_parcels)

geo_df.to_parquet('napa_public_parcels.parquet')


