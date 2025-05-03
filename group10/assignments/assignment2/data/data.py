import pandas as pd

# Define the data as a list of dictionaries
data = [
    (101, 1003, "play", "2023-09-01 08:23:55", "mobile", "US", "sess1A"),
    (102, 1005, "pause", "2023-09-01 09:15:42", "desktop", "EU", "sess2B"),
    (103, 1002, "skip", "2023-09-01 10:45:20", "tablet", "APAC", "sess3C"),
    (104, 1008, "forward", "2023-09-01 11:30:10", "mobile", "US", "sess4D"),
    (105, 1007, "play", "2023-09-01 12:05:30", "desktop", "EU", "sess5E"),
    (106, 1001, "pause", "2023-09-01 13:55:45", "tablet", "APAC", "sess6F"),
    (107, 1004, "skip", "2023-09-02 08:10:22", "mobile", "US", "sess7G"),
    (108, 1006, "forward", "2023-09-02 09:45:33", "desktop", "EU", "sess8H"),
    (109, 1009, "play", "2023-09-02 10:15:55", "tablet", "APAC", "sess9I"),
    (110, 1010, "pause", "2023-09-02 11:20:10", "mobile", "US", "sess10J"),
    (111, 1003, "skip", "2023-09-02 12:50:40", "desktop", "EU", "sess11K"),
    (112, 1005, "forward", "2023-09-02 13:35:25", "tablet", "APAC", "sess12L"),
    (113, 1007, "play", "2023-09-03 08:55:15", "mobile", "US", "sess13M"),
    (114, 1001, "pause", "2023-09-03 09:30:45", "desktop", "EU", "sess14N"),
    (115, 1008, "skip", "2023-09-03 10:10:55", "tablet", "APAC", "sess15O"),
    (116, 1006, "forward", "2023-09-03 11:45:20", "mobile", "US", "sess16P"),
    (117, 1002, "play", "2023-09-03 12:25:35", "desktop", "EU", "sess17Q"),
    (118, 1009, "pause", "2023-09-03 13:55:40", "tablet", "APAC", "sess18R"),
    (119, 1010, "skip", "2023-09-04 08:20:25", "mobile", "US", "sess19S"),
    (120, 1004, "forward", "2023-09-04 09:40:30", "desktop", "EU", "sess20T"),
    (121, 1005, "play", "2023-09-04 10:50:55", "tablet", "APAC", "sess21U"),
    (122, 1007, "pause", "2023-09-04 11:35:10", "mobile", "US", "sess22V"),
    (123, 1001, "skip", "2023-09-04 12:15:45", "desktop", "EU", "sess23W"),
    (124, 1003, "forward", "2023-09-05 08:05:30", "tablet", "APAC", "sess24X"),
    (125, 1008, "play", "2023-09-05 09:25:40", "mobile", "US", "sess25Y"),
    (126, 1006, "pause", "2023-09-05 10:35:55", "desktop", "EU", "sess26Z"),
    (127, 1002, "skip", "2023-09-05 11:50:20", "tablet", "APAC", "sess27A"),
    (128, 1009, "forward", "2023-09-06 08:15:45", "mobile", "US", "sess28B"),
    (129, 1010, "play", "2023-09-06 09:30:55", "desktop", "EU", "sess29C"),
    (130, 1004, "pause", "2023-09-06 10:40:20", "tablet", "APAC", "sess30D"),
]

# Create a DataFrame
df = pd.DataFrame(data, columns=["user_id", "content_id", "action", "timestamp", "device", "region", "session_id"])

# Convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Extract date
df["date"] = df["timestamp"].dt.date

# Save separate CSV files for each date
file_paths = {}
for date, group in df.groupby("date"):
    file_name = f"data_{date}.csv"
    group.drop(columns=["date"], inplace=True)
    file_paths[str(date)] = file_name
    group.to_csv(file_name, index=False)

file_paths
