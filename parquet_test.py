import pandas as pd
import time
from faker import Faker

fake = Faker()

MIL=1000000
NUM_OF_RECORDS=20*MIL
FOLDER="/tmp/out4/"
PARTITIONED_PATH=f"{FOLDER}partitioned_{NUM_OF_RECORDS}/"

NON_PARTITIONED_PATH_PREFIX=f"{FOLDER}non_partitioned_{NUM_OF_RECORDS}"
NON_PARTITIONED_PATH=f"{NON_PARTITIONED_PATH_PREFIX}.parquet"
NON_PARTITIONED_ROW_GROUPS_PATH=f"{NON_PARTITIONED_PATH_PREFIX}_row_groups.parquet"
NON_PARTITIONED_SORTED_PATH=f"{NON_PARTITIONED_PATH_PREFIX}_sorted.parquet"

ROW_GROUP_SIZE=MIL


print(f"Testing on {NUM_OF_RECORDS} records")

print(f"Creating fake data")
data = {
    'id': range(NUM_OF_RECORDS),  # Generate IDs from 1 to 100
    'name': [fake.name() for _ in range(NUM_OF_RECORDS)],
    'age': [fake.random_int(min=18, max=99) for _ in range(NUM_OF_RECORDS)],
    'state': [fake.state() for _ in range(NUM_OF_RECORDS)],
    'city': [fake.city() for _ in range(NUM_OF_RECORDS)],
    'street': [fake.street_address() for _ in range(NUM_OF_RECORDS)]
}

df = pd.DataFrame(data)



print("Creating non partitioned data")
df.to_parquet(path=NON_PARTITIONED_PATH)

print("Creating partitioned data")
df.to_parquet(path=PARTITIONED_PATH, partition_cols=['state'])

print("Creating non partitioned data and stating the num of row groups")
df.to_parquet(path=NON_PARTITIONED_ROW_GROUPS_PATH, row_group_size=ROW_GROUP_SIZE)

print("Creating non partitioned sorted data")
df.sort_values("state").to_parquet(path=NON_PARTITIONED_SORTED_PATH, row_group_size=ROW_GROUP_SIZE)



start_time = time.time()
df1 = pd.read_parquet(path=NON_PARTITIONED_PATH)
df1 = df1[df1['state']=='California']
runtime = (time.time()) - start_time
print(f"Reading not partitioned, filtering in runtime: {runtime:.6f} seconds")


start_time = time.time()
df2 = pd.read_parquet(path=PARTITIONED_PATH)
df2 = df2[df2['state']=='California']
runtime = (time.time()) - start_time
print(f"Reading partitioned, filtering in runtime: {runtime:.6f} seconds")


start_time = time.time()
df3 = pd.read_parquet(path=PARTITIONED_PATH, filters=[('state','==','California')])
runtime = (time.time()) - start_time
print(f"Reading partitioned, filtering by partitions(pushed down to read time): {runtime:.6f} seconds")



start_time = time.time()
df4 = pd.read_parquet(path=NON_PARTITIONED_ROW_GROUPS_PATH, filters=[('state','==','California')])
runtime = (time.time()) - start_time
print(f"Reading not partitioned, filtering by row groups: {runtime:.6f} seconds")



start_time = time.time()
df5 = pd.read_parquet(path=NON_PARTITIONED_SORTED_PATH, filters=[('state','==','California')])
runtime = (time.time()) - start_time
print(f"Reading not partitioned sorted, filtering by row groups: {runtime:.6f} seconds")


start_time = time.time()
df6 = pd.read_parquet(path=NON_PARTITIONED_SORTED_PATH, columns=["name", "state"])
runtime = (time.time()) - start_time
print(f"Reading not partitioned sorted, columns filtering: {runtime:.6f} seconds")


start_time = time.time()
df7 = pd.read_parquet(path=NON_PARTITIONED_SORTED_PATH, columns=["name", "state"], filters=[('state','==','California')])
runtime = (time.time()) - start_time
print(f"Reading not partitioned sorted, filtering by row groups and columns: {runtime:.6f} seconds")


start_time = time.time()
df8 = pd.read_parquet(path=PARTITIONED_PATH, columns=["name", "state"], filters=[('state','==','California')])
runtime = (time.time()) - start_time
print(f"Reading partitioned sorted, filtering by partitions and columns: {runtime:.6f} seconds")
