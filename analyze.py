import duckdb
import geopandas
import pandas as pd
from matplotlib.colors import to_rgba

from matplotlib import pyplot as plt

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

con = duckdb.connect(database=':memory:')
tbl = con.execute("""
    SELECT *
    FROM napa_public_parcels.parquet
""")

print(f"Input table description:")
for col in tbl.description:
    print(f"\t{col[0]} {col[1]}")

tbl = tbl.fetch_arrow_table()

deduped = con.execute("""
    SELECT
      apn_with_dash,
      land_use_code,
      acres
    FROM tbl
    GROUP BY apn_with_dash, land_use_code, acres
""").fetchdf()

print("\n\n\n\n")

print("=======================================================")
print("Printing total acreage by land use code...")
print("=======================================================")
acres_by_land_use_code = con.execute("""
    SELECT
      land_use_code,
      SUM(acres) as total_acres
    FROM deduped
    GROUP BY land_use_code
    ORDER BY total_acres DESC
""").fetchdf()
print(acres_by_land_use_code)

print("\n\n\n\n")
print("=======================================================")
print("Printing largest parcel sizes for each land use code...")
print("=======================================================")

largest_parcel_size_by_code = con.execute("""
    SELECT
      land_use_code,
      MAX(acres) as biggest_parcel_acreage
    FROM deduped
    GROUP BY land_use_code
    ORDER BY biggest_parcel_acreage DESC
""").fetchdf()
print(largest_parcel_size_by_code)

print("\n\n\n\n")
print("===========================================================================")
print("Printing the APNs with the largest acreage within each land use category...")
print("===========================================================================")

largest_parcels_by_code = con.execute("""
    SELECT
      largest_parcel_size_by_code.land_use_code,
      acres,
      array_agg(apn_with_dash) as apns
    FROM largest_parcel_size_by_code
    JOIN deduped
    ON
      largest_parcel_size_by_code.biggest_parcel_acreage = deduped.acres
      AND largest_parcel_size_by_code.land_use_code = deduped.land_use_code
    GROUP BY
      largest_parcel_size_by_code.land_use_code,
      acres
    ORDER BY acres DESC
""").fetchdf()
print(largest_parcels_by_code)


print("\n\n\n\n")
print("====================================================================================================")
print("Printing the original row for one of the larger single family residences (i.e. '011-400-014-000')...")
print("====================================================================================================")

largest_single_family_residence = con.execute("""
    SELECT * EXCLUDE (apn, apn_with_dash, geometry, land_use_code)
    FROM tbl
    WHERE apn_with_dash = '011-400-014-000'
""").fetchdf()
print(largest_single_family_residence)

parcels_ordered_by_size = con.execute("""
    WITH t as (
      SELECT
        *,
        land_use_code in ('11','111','111E','31','3101','32','3201','39','3901') as is_sfr
      FROM tbl
    )
    SELECT
      geometry,
      apn,
      acres,
      is_sfr,
      if(is_sfr, 'tab:blue', 'tab:gray') as color,
      percent_rank() over (
        PARTITION BY is_sfr
        ORDER BY acres
      ) as percent_rank
    FROM t
""").fetchdf()

parcels_ordered_by_size['geometry'] = geopandas.GeoSeries.from_wkt(parcels_ordered_by_size['geometry'])
gdf = geopandas.GeoDataFrame(parcels_ordered_by_size)
gdf['color_rgba'] = gdf.apply(
    lambda row: to_rgba(row['color'], alpha=row['percent_rank'] if row['is_sfr'] else 0.1), axis=1)
gdf.plot(color=gdf['color_rgba'])
plt.show()
